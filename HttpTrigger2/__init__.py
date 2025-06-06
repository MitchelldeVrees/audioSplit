import os
import io
import cgi
import json
import base64
import azure.functions as func
from pydub import AudioSegment
from openai import OpenAI
import platform

# ─────────────────────────────────────────────────────────────────────────────
this_folder = os.path.dirname(__file__)           # e.g. /Users/.../audioSplit/HttpTrigger2
bin_folder   = os.path.join(this_folder, "bin")   # e.g. /Users/.../audioSplit/HttpTrigger2/bin

current_system = platform.system().lower()   # "darwin" on Mac, "linux" on Azure

if current_system == "darwin":
    # On macOS: use the Homebrew-installed binaries
    AudioSegment.converter = "ffmpeg"
    AudioSegment.ffprobe   = "ffprobe"
    print("[DEBUG] macOS detected → using system ffmpeg/ffprobe")
else:
    # On Linux (i.e. Azure): use the bundled static binaries
    ffmpeg_path  = os.path.join(bin_folder, "ffmpeg")
    ffprobe_path = os.path.join(bin_folder, "ffprobe")
    os.environ["PATH"] = bin_folder + os.pathsep + os.environ.get("PATH", "")
    AudioSegment.converter = ffmpeg_path
    AudioSegment.ffprobe   = ffprobe_path
    print(f"[DEBUG] Linux detected → using ffmpeg at: {ffmpeg_path}")
    print(f"[DEBUG] Linux detected → using ffprobe at: {ffprobe_path}")
# ─────────────────────────────────────────────────────────────────────────────

client = OpenAI()

async def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1) Read body + Content-Type
    body_bytes   = req.get_body() or b""
    content_type = req.headers.get('content-type', "")
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
        'REQUEST_METHOD': 'POST',
        'CONTENT_TYPE': content_type
    }
    form = cgi.FieldStorage(fp=fp, environ=environ, keep_blank_values=True)

    # 4) Ensure we have audioFile field
    if 'audioFile' not in form:
        return func.HttpResponse('Missing form field "audioFile"', status_code=400)

    file_item = form['audioFile']  # a cgi.FieldStorage instance

    # 5) Read the uploaded bytes
    uploaded_filename = file_item.filename or "input.mp3"
    file_data         = file_item.file.read()
    if not file_data:
        return func.HttpResponse('Uploaded file is empty', status_code=400)

    # Determine the extension and whether we need to re-encode
    ext = os.path.splitext(uploaded_filename)[1].lower().lstrip(".")
    supported_formats = {"mp3", "mp4", "mpeg", "mpga", "m4a", "wav", "webm"}
    target_format = ext if ext in supported_formats else "mp3"

    # 6) Load into pydub.AudioSegment
    try:
        audio = AudioSegment.from_file(io.BytesIO(file_data))
    except Exception as e:
        return func.HttpResponse(f"Error loading audio: {e}", status_code=400)

    # 7) Split into 10-minute chunks
    chunk_length_ms = 10 * 60 * 1000  # 10 minutes in milliseconds
    total_length    = len(audio)
    transcripts     = []
    idx = 0

    for start_ms in range(0, total_length, chunk_length_ms):
        end_ms = min(start_ms + chunk_length_ms, total_length)
        segment = audio[start_ms:end_ms]

        # 8) Export each segment to an in-memory buffer.
        #    Use the original format when supported to avoid unnecessary re-encoding.
        buf = io.BytesIO()
        try:
            if target_format == "mp3":
                segment.export(buf, format="mp3", bitrate="128k")
            else:
                segment.export(buf, format=target_format)
        except Exception as export_err:
            return func.HttpResponse(f"Error exporting chunk: {export_err}", status_code=500)

        # 9) Transcribe via Whisper
        buf.seek(0)
        buf.name = f"chunk_{idx:03d}.{target_format}"
        try:
            result = client.audio.transcriptions.create(
                model="whisper-1",
                file=buf
            )
            transcripts.append(result.text or "")
        except Exception as trans_err:
            return func.HttpResponse(f"Error transcribing chunk {idx}: {trans_err}", status_code=500)

        idx += 1

    # 10) Combine all chunk texts into one big string
    full_text = " ".join(transcripts).strip()

    # 11) Return JSON with only the full transcript (no per-chunk data)
    return func.HttpResponse(
        body=json.dumps({"transcript": full_text}),
        status_code=200,
        mimetype="application/json"
    )