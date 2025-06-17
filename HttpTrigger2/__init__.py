import os
import io
import cgi
import json
import platform
import tempfile
import asyncio
import logging

import azure.functions as func
from pydub import AudioSegment
import fal_client  # the official Fal.ai SDK

# Configure root logger to INFO level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 1) Platform detection so we can pick the correct ffmpeg/ffprobe
this_folder = os.path.dirname(__file__)
bin_folder   = os.path.join(this_folder, "bin")
current_system = platform.system().lower()
if current_system == "darwin":
    AudioSegment.converter = "ffmpeg"
    AudioSegment.ffprobe   = "ffprobe"
    logger.debug("macOS detected → using system ffmpeg/ffprobe")
else:
    ffmpeg_path  = os.path.join(bin_folder, "ffmpeg")
    ffprobe_path = os.path.join(bin_folder, "ffprobe")
    os.environ["PATH"] = bin_folder + os.pathsep + os.environ.get("PATH", "")
    AudioSegment.converter = ffmpeg_path
    AudioSegment.ffprobe   = ffprobe_path
    logger.debug(f"Linux detected → using ffmpeg at: {ffmpeg_path}")
    logger.debug(f"Linux detected → using ffprobe at: {ffprobe_path}")
# ─────────────────────────────────────────────────────────────────────────────

# 2) Fal.ai settings
FAL_KEY   = os.environ.get("FAL_KEY", "").strip()
FAL_MODEL = "fal-ai/wizper"
if not FAL_KEY:
    logger.warning("FAL_KEY environment variable not set; Fal.ai calls will fail")
else:
    logger.info("Fal.ai API key found; ready to transcribe")

async def upload_and_transcribe_chunk(tmp_path: str) -> str:
    logger.info(f"Uploading chunk: {tmp_path}")
    try:
        audio_url = await asyncio.to_thread(fal_client.upload_file, tmp_path)
        logger.debug(f"Uploaded chunk URL: {audio_url}")
    except Exception as e:
        logger.error(f"Fal.ai upload_file failed: {e}")
        raise RuntimeError(f"Fal.ai upload_file failed: {e}") from e

    logger.info(f"Submitting transcription job for URL: {audio_url}")
    try:
        handler = await asyncio.to_thread(
            fal_client.submit,
            FAL_MODEL,
            {"audio_url": audio_url, "task": "transcribe", "language": "nl"},
        )
        request_id = handler.request_id
        logger.debug(f"Received request ID: {request_id}")
    except Exception as e:
        logger.error(f"Fal.ai submit failed: {e}")
        raise RuntimeError(f"Fal.ai submit(wizper) failed: {e}") from e

    logger.info(f"Polling result for request ID: {request_id}")
    try:
        result = await asyncio.to_thread(fal_client.result, FAL_MODEL, request_id)
        logger.debug(f"Received result: {result}")
    except Exception as e:
        logger.error(f"Fal.ai result failed: {e}")
        raise RuntimeError(f"Fal.ai result(wizper) failed: {e}") from e

    transcript = result.get("text", "")
    if transcript is None:
        logger.error(f"Fal.ai returned no 'text' field: {result}")
        raise RuntimeError(f"Fal.ai returned no 'text' field: {result}")

    logger.info(f"Chunk transcription complete, length={len(transcript)} chars")
    return transcript

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("/HttpTrigger2 invoked")

    # Read body & headers
    body_bytes   = req.get_body() or b""
    content_type = req.headers.get("content-type", "")
    logger.debug(f"Content-Type: {content_type}, Body size: {len(body_bytes)} bytes")

    if not content_type.startswith("multipart/form-data"):
        logger.error(f"Invalid Content-Type: {content_type}")
        return func.HttpResponse(
            "Invalid Content-Type. Must be multipart/form-data",
            status_code=400
        )

    # Parse multipart data
    try:
        fp = io.BytesIO(body_bytes)
        environ = {"REQUEST_METHOD": "POST", "CONTENT_TYPE": content_type}
        form = cgi.FieldStorage(fp=fp, environ=environ, keep_blank_values=True)
        logger.info("Parsed multipart/form-data")
    except Exception as e:
        logger.error(f"Failed to parse form-data: {e}")
        return func.HttpResponse(f"Error parsing form-data: {e}", status_code=400)

    if "audioFile" not in form:
        logger.error("Missing form field 'audioFile'")
        return func.HttpResponse('Missing form field "audioFile"', status_code=400)

    file_item = form["audioFile"]
    uploaded_filename = file_item.filename or "input"
    file_data         = file_item.file.read()
    logger.info(f"Received file: {uploaded_filename}, size={len(file_data)} bytes")

    if not file_data:
        logger.error("Uploaded file is empty")
        return func.HttpResponse('Uploaded file is empty', status_code=400)

    # Validate extension
    ext = os.path.splitext(uploaded_filename)[1].lower().lstrip('.')
    supported = {"mp3","mp4","mpeg","mpga","m4a","wav","webm"}
    if ext not in supported:
        logger.error(f"Unsupported extension: {ext}")
        return func.HttpResponse(
            f"Unsupported file extension '.{ext}'", status_code=400
        )
    logger.info(f"Processing as .{ext} format")

    # Load audio
    try:
        audio = AudioSegment.from_file(io.BytesIO(file_data), format=ext)
        logger.info(f"Loaded audio, duration={len(audio)} ms")
    except Exception as e:
        logger.error(f"Error loading audio: {e}")
        return func.HttpResponse(f"Error loading audio: {e}", status_code=400)

    # Split and transcribe
    chunk_ms  = 10 * 60 * 1000
    total_ms  = len(audio)
    logger.info(f"Splitting into chunks of {chunk_ms} ms; total length {total_ms} ms")

    tasks = []
    sem = asyncio.Semaphore(3)

    async def process(segment, idx):
        logger.debug(f"Exporting chunk {idx}")
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as tmp:
            path = tmp.name
            try:
                fmt = 'mp3' if ext=='mp3' else ext
                segment.export(tmp, format=fmt)
            except Exception as ex:
                logger.error(f"Error exporting chunk {idx}: {ex}")
                os.unlink(path)
                raise
        async with sem:
            try:
                return await upload_and_transcribe_chunk(path)
            finally:
                try: os.unlink(path)
                except: pass

    for idx, start in enumerate(range(0, total_ms, chunk_ms)):
        end = min(start+chunk_ms, total_ms)
        seg = audio[start:end]
        tasks.append(process(seg, idx))

    try:
        results = await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"Error during transcription: {e}")
        return func.HttpResponse(f"Error transcribing: {e}", status_code=500)

    full = " ".join(results).strip()
    logger.info(f"Transcription completed, total length={len(full)} chars")

    return func.HttpResponse(
        body=json.dumps({"transcript": full}),
        status_code=200,
        mimetype="application/json"
    )
