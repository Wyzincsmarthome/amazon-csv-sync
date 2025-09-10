# app_flask.py
# Flask API para importar CSV (ficheiro local ou URL) e validar/parquear para data/visiotech.csv
# Devolve SEMPRE JSON (inclui handlers para 413/500) e suporta ficheiros grandes em Codespaces.

import os
import io
import csv
import traceback
from datetime import datetime
from typing import Optional

import requests
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename

try:
    import pandas as pd
except Exception:
    pd = None  # pandas é opcional para validações extra

# ---------- Config ----------
app = Flask(__name__)
# aumenta o limite de upload (ex.: 200 MB)
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_CONTENT_LENGTH_MB", "200")) * 1024 * 1024
# diretório onde vamos guardar o CSV final
DATA_DIR = os.getenv("DATA_DIR", "data")
DEFAULT_CSV_PATH = os.getenv("CSV_INPUT", os.path.join(DATA_DIR, "visiotech.csv"))
os.makedirs(DATA_DIR, exist_ok=True)

ALLOWED_EXTS = {".csv", ".txt"}  # aceita .csv/.txt
TIMEOUT = 60

# ---------- Utils ----------
def _is_allowed(filename: str) -> bool:
    name = filename.lower()
    return any(name.endswith(ext) for ext in ALLOWED_EXTS)

def _save_bytes_to_path(content: bytes, dest_path: str) -> None:
    with open(dest_path, "wb") as f:
        f.write(content)

def _try_read_csv_bytes(content: bytes) -> dict:
    """
    Verifica rapidamente se o CSV é legível.
    Retorna {"ok": bool, "rows": int, "cols": int, "sample": list[str]}.
    """
    try:
        sample = content[:4096].decode("utf-8", errors="replace")
        # contar colunas por separador provável
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        reader = csv.reader(io.StringIO(content.decode("utf-8", errors="replace")), dialect)
        rows = 0
        cols = 0
        sample_rows = []
        for i, row in enumerate(reader):
            rows += 1
            cols = max(cols, len(row))
            if i < 3:
                sample_rows.append(row)
            if i >= 10000:  # não precisamos ler tudo
                break
        return {"ok": True, "rows": rows, "cols": cols, "sample": sample_rows}
    except Exception:
        # fallback: se houver pandas, tenta ler
        if pd is not None:
            try:
                df = pd.read_csv(io.BytesIO(content), sep=None, engine="python")
                return {"ok": True, "rows": int(df.shape[0]), "cols": int(df.shape[1]), "sample": df.head(3).astype(str).values.tolist()}
            except Exception:
                pass
        return {"ok": False, "rows": 0, "cols": 0, "sample": []}

def _download_url(url: str) -> bytes:
    r = requests.get(url, timeout=TIMEOUT, stream=True)
    r.raise_for_status()
    # recusamos HTML “disfarçado” (evita JSON.parse('<html>')
    ctype = r.headers.get("Content-Type","").lower()
    if "text/html" in ctype:
        # pode ser um erro/protetor de link; tenta mesmo assim mas valida depois
        content = r.content
    else:
        content = r.content
    return content

# ---------- Error Handlers (respostas sempre JSON) ----------
@app.errorhandler(413)
def too_large(e):
    return jsonify({
        "ok": False,
        "error": "UPLOAD_TOO_LARGE",
        "message": f"Ficheiro excede o limite configurado ({app.config['MAX_CONTENT_LENGTH']//(1024*1024)} MB)."
    }), 413

@app.errorhandler(Exception)
def on_error(e):
    # devolve JSON, não HTML, para evitar "Unexpected token '<'"
    return jsonify({
        "ok": False,
        "error": "SERVER_ERROR",
        "message": str(e),
        "trace": traceback.format_exc(limit=3)
    }), 500

# ---------- Health ----------
@app.get("/health")
def health():
    return jsonify({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

# ---------- Upload por FICHEIRO ----------
@app.post("/upload_csv")
def upload_csv():
    """
    Espera um form multipart com 'file' (input type=file).
    Responde JSON com estatísticas e caminho final (data/visiotech.csv por defeito).
    """
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "NO_FILE", "message": "Falta o campo 'file' no formulário (multipart/form-data)."}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"ok": False, "error": "EMPTY_FILENAME", "message": "Nome de ficheiro vazio."}), 400

    if not _is_allowed(file.filename):
        return jsonify({"ok": False, "error": "BAD_EXTENSION", "message": f"Extensão não permitida. Use: {', '.join(ALLOWED_EXTS)}"}), 400

    filename = secure_filename(file.filename)
    content = file.read()
    meta = _try_read_csv_bytes(content)
    if not meta["ok"]:
        return jsonify({"ok": False, "error": "INVALID_CSV", "message": "Não foi possível ler o CSV (encoding/delimitador)."}), 400

    dest = DEFAULT_CSV_PATH
    _save_bytes_to_path(content, dest)

    return jsonify({
        "ok": True,
        "source": f"upload:{filename}",
        "saved_to": dest,
        "rows": meta["rows"],
        "cols": meta["cols"],
        "sample": meta["sample"]
    })

# ---------- Upload por URL ----------
@app.post("/upload_csv_url")
def upload_csv_url():
    """
    Espera JSON: {"csv_url": "https://.../ficheiro.csv"}
    Faz download no servidor e guarda em data/visiotech.csv (ou onde CSV_INPUT apontar).
    """
    data = request.get_json(silent=True) or {}
    url = (data.get("csv_url") or "").strip()

    if not url:
        return jsonify({"ok": False, "error": "NO_URL", "message": "Falta 'csv_url' no corpo JSON."}), 400

    content = _download_url(url)
    meta = _try_read_csv_bytes(content)
    if not meta["ok"]:
        return jsonify({"ok": False, "error": "INVALID_CSV", "message": "Conteúdo recebido não é CSV válido."}), 400

    dest = DEFAULT_CSV_PATH
    _save_bytes_to_path(content, dest)

    return jsonify({
        "ok": True,
        "source": f"url:{url}",
        "saved_to": dest,
        "rows": meta["rows"],
        "cols": meta["cols"],
        "sample": meta["sample"]
    })

# ---------- Arranque local / Codespaces ----------
if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", os.getenv("PORT", "5000")))
    app.run(host=host, port=port)
