# app_flask.py
import os, io, csv, traceback, requests
from datetime import datetime
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename

app = Flask(__name__)
# <<< AQUI DEFINIMOS O LIMITE DE UPLOAD >>>
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_CONTENT_LENGTH_MB", "200")) * 1024 * 1024  # 200 MB por defeito

DATA_DIR = os.getenv("DATA_DIR", "data")
DEFAULT_CSV_PATH = os.getenv("CSV_INPUT", os.path.join(DATA_DIR, "visiotech.csv"))
os.makedirs(DATA_DIR, exist_ok=True)
ALLOWED_EXTS = {".csv", ".txt"}

def _is_allowed(filename: str) -> bool:
    return any(filename.lower().endswith(ext) for ext in ALLOWED_EXTS)

def _save_bytes_to_path(content: bytes, dest_path: str) -> None:
    with open(dest_path, "wb") as f:
        f.write(content)

def _try_read_csv_bytes(content: bytes):
    try:
        sample = content[:4096].decode("utf-8", errors="replace")
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        reader = csv.reader(io.StringIO(content.decode("utf-8", errors="replace")), dialect)
        rows, cols, sample_rows = 0, 0, []
        for i, row in enumerate(reader):
            rows += 1
            cols = max(cols, len(row))
            if i < 3: sample_rows.append(row)
            if i >= 10000: break
        return {"ok": True, "rows": rows, "cols": cols, "sample": sample_rows}
    except Exception:
        return {"ok": False, "rows": 0, "cols": 0, "sample": []}

def _download_url(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

@app.errorhandler(413)
def too_large(e):
    return jsonify({"ok": False, "error": "UPLOAD_TOO_LARGE",
                    "message": f"Ficheiro excede o limite ({app.config['MAX_CONTENT_LENGTH']//(1024*1024)} MB)."}), 413

@app.errorhandler(Exception)
def on_error(e):
    return jsonify({"ok": False, "error": "SERVER_ERROR", "message": str(e),
                    "trace": traceback.format_exc(limit=3)}), 500

@app.get("/health")
def health():
    return jsonify({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

@app.post("/upload_csv")
def upload_csv():
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "NO_FILE", "message": "Falta o campo 'file'."}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"ok": False, "error": "EMPTY_FILENAME", "message": "Nome de ficheiro vazio."}), 400
    if not _is_allowed(file.filename):
        return jsonify({"ok": False, "error": "BAD_EXTENSION",
                        "message": f"Extensão não permitida: {file.filename}"}), 400
    content = file.read()
    meta = _try_read_csv_bytes(content)
    if not meta["ok"]:
        return jsonify({"ok": False, "error": "INVALID_CSV", "message": "Não foi possível ler o CSV."}), 400
    _save_bytes_to_path(content, DEFAULT_CSV_PATH)
    return jsonify({"ok": True, "source": file.filename, "saved_to": DEFAULT_CSV_PATH,
                    "rows": meta["rows"], "cols": meta["cols"], "sample": meta["sample"]})

@app.post("/upload_csv_url")
def upload_csv_url():
    data = request.get_json(silent=True) or {}
    url = (data.get("csv_url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "NO_URL", "message": "Falta 'csv_url' no corpo JSON."}), 400
    content = _download_url(url)
    meta = _try_read_csv_bytes(content)
    if not meta["ok"]:
        return jsonify({"ok": False, "error": "INVALID_CSV",
                        "message": "Conteúdo recebido não é CSV válido."}), 400
    _save_bytes_to_path(content, DEFAULT_CSV_PATH)
    return jsonify({"ok": True, "source": url, "saved_to": DEFAULT_CSV_PATH,
                    "rows": meta["rows"], "cols": meta["cols"], "sample": meta["sample"]})

if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", os.getenv("PORT", "5000")))
    app.run(host=host, port=port)
