import os
import io
import cgi
import json
import platform
import tempfile
import asyncio

import azure.functions as func
from pydub import AudioSegment
import fal_client  # the official Fal.ai SDK

# ─────────────────────────────────────────────────────────────────────────────
# 1) Platform detection so we can pick the correct ffmpeg/ffprobe
this_folder = os.path.dirname(__file__)           # e.g. /Users/.../audioSplit/HttpTrigger2
bin_folder   = os.path.join(this_folder, "bin")   # e.g. /Users/.../audioSplit/HttpTrigger2/bin

current_system = platform.system().lower()   # "darwin" on Mac, "linux" on Azure

if current_system == "darwin":
    # On macOS: assume Homebrew-installed ffmpeg/ffprobe are on PATH
    AudioSegment.converter = "ffmpeg"
    AudioSegment.ffprobe   = "ffprobe"
    print("[DEBUG] macOS detected → using system ffmpeg/ffprobe")
else:
    # On Linux (Azure): use the bundled static binaries
    ffmpeg_path  = os.path.join(bin_folder, "ffmpeg")
    ffprobe_path = os.path.join(bin_folder, "ffprobe")
    os.environ["PATH"] = bin_folder + os.pathsep + os.environ.get("PATH", "")
    AudioSegment.converter = ffmpeg_path
    AudioSegment.ffprobe   = ffprobe_path
    print(f"[DEBUG] Linux detected → using ffmpeg at: {ffmpeg_path}")
    print(f"[DEBUG] Linux detected → using ffprobe at: {ffprobe_path}")
# ─────────────────────────────────────────────────────────────────────────────

# 2) Fal.ai (wizper) settings
FAL_KEY     = os.environ.get("FAL_KEY", "").strip()
FAL_MODEL   = "fal-ai/wizper"
if not FAL_KEY:
    print("[WARNING] FAL_KEY environment variable not set; Fal.ai calls will fail")


async def upload_and_transcribe_chunk(tmp_path: str) -> str:
    """
    1) Upload the file at tmp_path to Fal.ai storage.
    2) Submit a wizper transcription job using the returned URL.
    3) Wait for the result and return the 'text' field of the response.
    """
    # 1) Upload the chunk to Fal.ai’s storage
    try:
        # fal_client.upload_file is synchronous; wrap it in to_thread
        audio_url = await asyncio.to_thread(fal_client.upload_file, tmp_path)
    except Exception as e:
        raise RuntimeError(f"Fal.ai upload_file failed: {e}") from e

    # 2) Submit a wizper job
    handler = None
    try:
        # fal_client.submit is synchronous as well
        handler = await asyncio.to_thread(
            fal_client.submit,
            FAL_MODEL,
            {"audio_url": audio_url,  
             "task": "transcribe",
                  
},
        )
    except Exception as e:
        raise RuntimeError(f"Fal.ai submit(wizper) failed: {e}") from e

    request_id = handler.request_id

    # 3) Wait for the result (this polls until the job is complete)
    result = None
    try:
        result = await asyncio.to_thread(
            fal_client.result,
            FAL_MODEL,
            request_id,
        )
    except Exception as e:
        raise RuntimeError(f"Fal.ai result(wizper) failed: {e}") from e

    # The JSON schema for wizper’s output includes a "text" field
    transcript = result.get("text", "")
    if transcript is None:
        raise RuntimeError(f"Fal.ai returned no 'text' field: {result}")
    return transcript


async def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1) Read body + Content-Type
    body_bytes   = req.get_body() or b""
    content_type = req.headers.get("content-type", "")
    print("Calling split_audio with:")

    # 2) Must be multipart/form-data
    if not content_type.startswith("multipart/form-data"):
        return func.HttpResponse(
            "Invalid Content-Type. Must be multipart/form-data",
            status_code=400
        )

    # 3) Parse multipart/form-data via cgi.FieldStorage
    fp = io.BytesIO(body_bytes)
    environ = {
        "REQUEST_METHOD": "POST",
        "CONTENT_TYPE": content_type
    }
    form = cgi.FieldStorage(fp=fp, environ=environ, keep_blank_values=True)

    # 4) Ensure we have audioFile field
    if "audioFile" not in form:
        return func.HttpResponse('Missing form field "audioFile"', status_code=400)

    file_item = form["audioFile"]  # a cgi.FieldStorage instance

    # 5) Read the uploaded bytes
    uploaded_filename = file_item.filename or "input.mp3"
    file_data         = file_item.file.read()
    if not file_data:
        return func.HttpResponse('Uploaded file is empty', status_code=400)

    # 6) Determine extension & target_format
    ext = os.path.splitext(uploaded_filename)[1].lower().lstrip(".")
    supported_formats = {"mp3", "mp4", "mpeg", "mpga", "m4a", "wav", "webm"}
    if ext not in supported_formats:
        return func.HttpResponse(
            f"Unsupported file extension '.{ext}'. Allowed: {', '.join(sorted(supported_formats))}",
            status_code=400,
        )

    target_format = ext

    # 7) Load into pydub.AudioSegment
    try:
        audio = AudioSegment.from_file(io.BytesIO(file_data), format=target_format)
    except Exception as e:
        return func.HttpResponse(f"Error loading audio: {e}", status_code=400)

    # 8) Split into 10‐minute chunks (in milliseconds)
    chunk_length_ms = 10 * 60 * 1000
    total_length    = len(audio)

    transcripts = []
    tasks = []
    sem = asyncio.Semaphore(3)  # limit concurrency to 3 chunks at a time

    async def process_segment(segment: AudioSegment, idx: int) -> str:
        """
        Export this segment to a temporary file, then upload & transcribe via Fal.ai.
        """
        # a) Export segment to a NamedTemporaryFile on disk
        suffix = f".{target_format}"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = tmp.name
            try:
                ffmpeg_format = {
                    "m4a": "ipod",
                }.get(target_format, target_format)
                if target_format == "mp3":
                    segment.export(tmp, format="mp3", bitrate="128k")
                else:
                    segment.export(tmp, format=ffmpeg_format)
            except Exception as export_err:
                os.unlink(tmp_path)  # cleanup
                raise RuntimeError(f"Error exporting chunk {idx}: {export_err}") from export_err

        # b) Ensure other tasks can start only when semaphore allows
        async with sem:
            try:
                transcript = await upload_and_transcribe_chunk(tmp_path)
            finally:
                # Always delete the temp file, even if transcription failed
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            return transcript

    # 9) Spawn a task for each chunk
    for idx, start_ms in enumerate(range(0, total_length, chunk_length_ms)):
        end_ms  = min(start_ms + chunk_length_ms, total_length)
        segment = audio[start_ms:end_ms]
        tasks.append(process_segment(segment, idx))

    # 10) Await all transcription tasks
    try:
        transcripts = await asyncio.gather(*tasks)
    except Exception as e:
        return func.HttpResponse(f"Error transcribing: {e}", status_code=500)

    # 11) Combine all chunk texts into one big string
    full_text = " ".join(transcripts).strip()

    # 12) Return JSON with only the full transcript
    return func.HttpResponse(
        body=json.dumps({"transcript": full_text}),
        status_code=200,
        mimetype="application/json"
    )
