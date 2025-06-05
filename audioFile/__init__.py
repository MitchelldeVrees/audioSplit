
from pydub import AudioSegment
import os

this_folder = os.path.dirname(__file__)
bin_folder = os.path.join(this_folder, "bin")

# Tell pydub exactly where ffmpeg.exe lives:
AudioSegment.converter = os.path.join(bin_folder, "ffmpeg.exe")

# Tell pydub where ffprobe.exe lives (for format analysis):
AudioSegment.ffprobe   = os.path.join(bin_folder, "ffprobe.exe")

os.environ["PATH"] = bin_folder + os.pathsep + os.environ.get("PATH", "")

print(f"[DEBUG] pydub will use ffmpeg at: {AudioSegment.converter}")
print(f"[DEBUG] pydub will use ffprobe at: {AudioSegment.ffprobe}")
print(f"[DEBUG] os.environ['PATH'] starts with: {os.environ['PATH'].split(os.pathsep)[0]}")

import azure.functions as func
import cgi
import io
import json
import base64
async def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1) Read the raw request body and Content-Type
    body_bytes   = req.get_body() or b""
    content_type = req.headers.get('content-type', "")

    # 2) Validate Content-Type
    if not content_type.startswith("multipart/form-data"):
        return func.HttpResponse(
            "Invalid Content-Type. Must be multipart/form-data",
            status_code=400
        )

    # 3) Parse multipart/form-data using cgi.FieldStorage
    fp = io.BytesIO(body_bytes)
    environ = {
        'REQUEST_METHOD': 'POST',
        'CONTENT_TYPE': content_type
    }
    form = cgi.FieldStorage(fp=fp, environ=environ, keep_blank_values=True)

    # 4) Ensure "audioFile" field exists
    if 'audioFile' not in form:
        return func.HttpResponse(
            'Missing form field "audioFile"',
            status_code=400
        )

    file_item = form['audioFile']  # cgi.FieldStorage item

    # 5) Read the uploaded bytes
    uploaded_filename = file_item.filename or "input.mp3"
    file_data         = file_item.file.read()
    if not file_data:
        return func.HttpResponse(
            'Uploaded file is empty',
            status_code=400
        )

    # 6) Load into pydub
    try:
        audio = AudioSegment.from_file(io.BytesIO(file_data))
    except Exception as e:
        return func.HttpResponse(
            f"Error loading audio: {e}",
            status_code=400
        )

    # 7) Split into 10-minute chunks (adjust as desired)
    chunk_length_ms = 10 * 60 * 1000  # 10 minutes in milliseconds
    total_length   = len(audio)
    chunks = []
    idx = 0
    for start_ms in range(0, total_length, chunk_length_ms):
        end_ms = min(start_ms + chunk_length_ms, total_length)
        segment = audio[start_ms:end_ms]

        # 8) Export each chunk to an in-memory buffer as MP3
        buf = io.BytesIO()
        try:
            segment.export(buf, format="mp3", bitrate="128k")
        except Exception as export_err:
            return func.HttpResponse(
                f"Error exporting chunk: {export_err}",
                status_code=500
            )

        # 9) Base64‐encode the buffer
        buf.seek(0)
        b64data = base64.b64encode(buf.read()).decode('utf-8')

        chunk_name = f"chunk_{idx:03d}.mp3"
        chunks.append({
            "name": chunk_name,
            "mime": "audio/mpeg",
            "data": b64data
        })
        idx += 1

    # 10) Return a JSON array of chunks
    return func.HttpResponse(
        body=json.dumps(chunks),
        status_code=200,
        mimetype="application/json"
    )
