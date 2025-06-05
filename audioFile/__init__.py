import os
import io
import cgi
import json
import base64
import azure.functions as func
from pydub import AudioSegment
from openai import OpenAI

# ─────────────────────────────────────────────────────────────────────────────
# 1) Configure pydub to use the bundled ffmpeg/ffprobe (inside split_audio/bin)
this_folder = os.path.dirname(__file__)
bin_folder   = os.path.join(this_folder, "bin")
ffmpeg_path  = os.path.join(bin_folder, "ffmpeg.exe")
ffprobe_path = os.path.join(bin_folder, "ffprobe.exe")

# Prepend bin_folder to PATH so ffmpeg/ffprobe are found automatically
os.environ["PATH"] = bin_folder + os.pathsep + os.environ.get("PATH", "")

# Tell pydub exactly where to find those executables:
AudioSegment.converter = ffmpeg_path
AudioSegment.ffprobe   = ffprobe_path

print(f"[DEBUG] pydub will use ffmpeg at: {AudioSegment.converter}")
print(f"[DEBUG] pydub will use ffprobe at: {AudioSegment.ffprobe}")
print(f"[DEBUG] os.environ['PATH'] starts with: {os.environ['PATH'].split(os.pathsep)[0]}")
# ─────────────────────────────────────────────────────────────────────────────

# 2) Create an OpenAI client (will read OPENAI_API_KEY from the environment by default)
client = OpenAI()

async def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1) Read body + Content-Type
    body_bytes   = req.get_body() or b""
    content_type = req.headers.get('content-type', "")

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

        # 8) Export each segment to an in-memory MP3
        buf = io.BytesIO()
        try:
            segment.export(buf, format="mp3", bitrate="128k")
        except Exception as export_err:
            return func.HttpResponse(f"Error exporting chunk: {export_err}", status_code=500)

        # 9) Transcribe via Whisper
        buf.seek(0)
        buf.name = f"chunk_{idx:03d}.mp3"
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
