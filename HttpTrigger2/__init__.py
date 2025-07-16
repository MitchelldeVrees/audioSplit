import os
import io
import cgi
import json
import logging
import tempfile
import asyncio

import azure.functions as func
import fal_client
import av

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODEL = "fal-ai/whisper"
LANG  = "nl"

async def transcribe_file(path: str, language: str = LANG) -> str:
    """
    Uploads `path` to Fal.ai and blocks until run() returns the final result.
    """
    # 1) upload
    try:
        url = fal_client.upload_file(path)
        logger.debug(f"Uploaded to Fal.ai → {url}")
    except Exception as e:
        logger.error("Fal.ai upload failed", exc_info=e)
        raise RuntimeError(f"Upload failed: {e}") from e

    # 2) blocking run()
    try:
        result = await asyncio.to_thread(
            fal_client.run,
            MODEL,
            {"audio_url": url, "task": "transcribe"},
        )
    except Exception as e:
        logger.error("Fal.ai run() failed", exc_info=e)
        raise RuntimeError(f"Transcription failed: {e}") from e

    text = result.get("text")
    if not text:
        raise RuntimeError(f"No transcript returned: {result}")
    return text

async def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("HttpTrigger (PyAV demux → faststart M4A → Fal.ai run) invoked")

    # 1) require multipart/form-data
    content_type = req.headers.get("content-type", "")
    if "form-data" not in content_type.lower():
        return func.HttpResponse("Content-Type must be multipart/form-data", status_code=400)

    # 2) parse form
    try:
        buf = io.BytesIO(req.get_body() or b"")
        form = cgi.FieldStorage(fp=buf, environ={
            "REQUEST_METHOD": "POST", "CONTENT_TYPE": content_type
        })
    except Exception as e:
        logger.error("Form parse error", exc_info=e)
        return func.HttpResponse(f"Form parse error: {e}", status_code=400)

    if "audioFile" not in form:
        return func.HttpResponse('Missing form field "audioFile"', status_code=400)

    # 3) save upload to temp file
    file_item = form["audioFile"]
    data      = file_item.file.read()
    if not data:
        return func.HttpResponse("Uploaded file is empty", status_code=400)

    original_name = file_item.filename or "upload"
    ext           = os.path.splitext(original_name)[1].lower().lstrip(".")
    in_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}")
    try:
        in_tmp.write(data)
        in_tmp.flush()
        in_path = in_tmp.name
    finally:
        in_tmp.close()
    logger.info(f"Wrote upload to {in_path}")

    # 4) if MP4, demux AAC → M4A with faststart
    out_path = in_path
    if ext == "mp4":
        out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".m4a")
        out_path = out_tmp.name
        out_tmp.close()

        try:
            cin = av.open(in_path)
            audio_stream = next(s for s in cin.streams if s.type == "audio")
            codec_name   = audio_stream.codec_context.name

            # tell FFmpeg to put the 'moov' atom up front
            cout = av.open(
                out_path,
                mode="w",
                format="mp4",
                options={"movflags": "+faststart"}
            )
            sout = cout.add_stream(codec_name, rate=audio_stream.codec_context.sample_rate)

            # packet‐copy into the new container
            for packet in cin.demux(audio_stream):
                packet.stream = sout
                cout.mux(packet)

            cin.close()
            cout.close()
            logger.info(f"Demuxed & faststarted → {out_path}")

        except Exception as e:
            logger.error("PyAV remux failed", exc_info=e)
            for p in (in_path, out_path):
                try: os.unlink(p)
                except: pass
            return func.HttpResponse(f"Audio extraction failed: {e}", status_code=500)

        finally:
            # remove original .mp4
            try: os.unlink(in_path)
            except: pass

    # 5) transcribe & cleanup
    try:
        transcript = await transcribe_file(out_path)
    except Exception as e:
        logger.error("Transcription pipeline failed", exc_info=e)
        try: os.unlink(out_path)
        except: pass
        return func.HttpResponse(f"Error: {e}", status_code=500)

    try:
        os.unlink(out_path)
    except: pass

    # 6) return the transcript
    return func.HttpResponse(
        body=json.dumps({"transcript": transcript}),
        status_code=200,
        mimetype="application/json"
    )
