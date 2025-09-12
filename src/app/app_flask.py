from __future__ import annotations
from flask import Flask, jsonify
from app.settings import SETTINGS
from app.logging_setup import setup_logger

log = setup_logger()
app = Flask(__name__)

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "simulate": SETTINGS.app_simulate,
        "marketplace": SETTINGS.marketplace_id
    })

if __name__ == "__main__":
    # Necessário no Codespaces para expor a porta
    app.run(host="0.0.0.0", port=8000)
