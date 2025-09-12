#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import traceback
import pandas as pd
import requests
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from dotenv import load_dotenv

# módulos do teu repo
from csv_processor_visiotech import process_csv, load_cfg
from asin_resolver import resolve_asin
from amazon_client import patch_listings_item, put_listings_item

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "dev-secret")

# aceitar uploads grandes (ex.: 300 MB) para evitar 413
MAX_MB = int(os.getenv("MAX_CONTENT_LENGTH_MB", "300"))
app.config["MAX_CONTENT_LENGTH"] = MAX_MB * 1024 * 1024

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def _bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name, str(default)).lower().strip()
    return v in ("1", "true", "yes", "on")

@app.context_processor
def inject_flags():
    return {
        "DRY_RUN": _bool_env("DRY_RUN", True),
        "MAX_MB": MAX_MB
    }

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload():
    try:
        f = request.files.get("file")
        if not f or f.filename.strip() == "":
            flash("Seleciona um ficheiro CSV.", "warning")
            return redirect(url_for("index"))
        dest = os.path.join(DATA_DIR, "visiotech.csv")
        f.save(dest)
        flash(f"Upload concluído: {f.filename}", "success")
    except Exception as e:
        flash(f"Erro no upload: {e}", "danger")
    return redirect(url_for("classify"))

@app.route("/upload_by_url", methods=["POST"])
def upload_by_url():
    try:
        url = request.form.get("csv_url", "").strip()
        if not url:
            flash("Indica um URL direto para CSV.", "warning")
            return redirect(url_for("index"))
        dest = os.path.join(DATA_DIR, "visiotech.csv")
        with requests.get(url, stream=True, timeout=180) as r:
            r.raise_for_status()
            with open(dest, "wb") as out:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        out.write(chunk)
        flash("Download do CSV concluído.", "success")
    except Exception as e:
        flash(f"Erro a descarregar CSV: {e}", "danger")
    return redirect(url_for("classify"))

@app.route("/classified")
def classify():
    """
    Mostra tabela com: SKU, EAN, stock, cost_price, floor_price, preview_price, selling_price, ASIN (preview).
    Não envia nada para a Amazon; apenas visualização/validação.
    """
    try:
        csv_path = os.path.join(DATA_DIR, "visiotech.csv")
        if not os.path.isfile(csv_path):
            flash("Falta o CSV (faz upload ou import por URL).", "warning")
            return redirect(url_for("index"))

        cfg = load_cfg()  # lê config.json (margens, IVA, etc.)
        df = process_csv(csv_path, cfg).fillna("")

        # tentar resolver ASIN para preview (sem falhar a página)
        seller_id = os.getenv("SELLER_ID", "")
        rows = []
        for _, r in df.iterrows():
            sku = str(r.get("sku", "")).strip()
            ean = str(r.get("ean", "")).strip()
            brand = str(r.get("brand", "")).strip()
            title = str(r.get("title", "")).strip()
            stock = int(str(r.get("stock", "0")).strip() or 0)
            cost_price = r.get("cost_price", "")
            floor_price = r.get("floor_price", "")
            preview_price = r.get("preview_price", "")
            selling_price = r.get("selling_price", r.get("preview_price", ""))

            asin = "n/a"
            try:
                asin_info = resolve_asin(sku=sku, name=title, brand=brand, ean=ean, seller_id=seller_id)
                if isinstance(asin_info, dict):
                    asin = (asin_info.get("asin") or "").strip() or "n/a"
                elif isinstance(asin_info, str):
                    asin = asin_info.strip() or "n/a"
            except Exception:
                asin = "n/a"

            rows.append({
                "sku": sku, "ean": ean, "stock": stock,
                "cost_price": cost_price, "floor_price": floor_price,
                "preview_price": preview_price, "selling_price": selling_price,
                "asin": asin
            })

        # guardar também CSV para análise offline
        out_path = os.path.join(DATA_DIR, "produtos_processados.csv")
        pd.DataFrame(rows).to_csv(out_path, index=False, encoding="utf-8")

        return render_template("classified.html", rows=rows[:1000], total=len(rows))
    except Exception as e:
        traceback.print_exc()
        flash(f"Erro a classificar: {e}", "danger")
        return redirect(url_for("index"))

@app.route("/sync", methods=["POST"])
def sync_to_amazon():
    """
    Envia as alterações para a Amazon se DRY_RUN=false; caso contrário, simula e guarda data/sync_result.csv.
    """
    try:
        dry_run = _bool_env("DRY_RUN", True)
        seller_id = os.getenv("SELLER_ID", "")
        marketplace_id = os.getenv("MARKETPLACE_ID", "")

        csv_path = os.path.join(DATA_DIR, "visiotech.csv")
        if not os.path.isfile(csv_path):
            flash("Falta o CSV (faz upload/import primeiro).", "warning")
            return redirect(url_for("index"))

        cfg = load_cfg()
        df = process_csv(csv_path, cfg).fillna("")

        out_rows = []
        for _, r in df.iterrows():
            sku = str(r.get("sku", "")).strip()
            ean = str(r.get("ean", "")).strip()
            brand = str(r.get("brand", "")).strip()
            title = str(r.get("title", "")).strip()
            stock = int(str(r.get("stock", "0")).strip() or 0)
            try:
                price = float(str(r.get("selling_price") or r.get("preview_price") or 0.0).replace(",", "."))
            except Exception:
                price = 0.0

            asin = ""
            try:
                asin_info = resolve_asin(sku=sku, name=title, brand=brand, ean=ean, seller_id=seller_id)
                if isinstance(asin_info, dict):
                    asin = (asin_info.get("asin") or "").strip()
                elif isinstance(asin_info, str):
                    asin = asin_info.strip()
            except Exception:
                asin = ""

            if asin:
                action = "PATCH"
                if dry_run:
                    status, msg = "DRY_RUN", f"PATCH {sku} asin={asin} qty={stock} price={price:.2f}"
                else:
                    # Usa a tua amazon_client com parâmetros seller/marketplace
                    resp = patch_listings_item(
                        seller_id=seller_id,
                        sku=sku,
                        marketplace_id=marketplace_id,
                        price=price,
                        quantity=stock
                    )
                    status, msg = "OK", str(resp)[:200]
            else:
                action = "PUT"
                # atributos mínimos (ajusta product_type se tiveres mapeamento por categoria)
                attributes = {
                    "brand": brand,
                    "item_name": title,
                    "external_product_id": ean,
                    "external_product_id_type": "EAN",
                    "purchasable_offer": {
                        "currency": "EUR",
                        "our_price": [{
                            "schedule": [{
                                "value_with_tax": {"amount": f"{price:.2f}", "currency": "EUR"}
                            }]
                        }]
                    },
                    "fulfillment_availability": [{
                        "fulfillment_channel_code": "DEFAULT",
                        "quantity": stock
                    }]
                }
                product_type = "product"
                if dry_run:
                    status, msg = "DRY_RUN", f"PUT {sku} ean={ean} qty={stock} price={price:.2f}"
                else:
                    resp = put_listings_item(
                        seller_id=seller_id,
                        sku=sku,
                        marketplace_id=marketplace_id,
                        price=price,
                        quantity=stock,
                        ean=ean
                    )
                    status, msg = "OK", str(resp)[:200]

            out_rows.append({
                "sku": sku, "ean": ean, "stock": stock, "final_price": price,
                "asin": asin if asin else "n/a", "action": action, "status": status, "message": msg
            })

        # relatório
        out_path = os.path.join(DATA_DIR, "sync_result.csv")
        pd.DataFrame(out_rows).to_csv(out_path, index=False, encoding="utf-8")

        flash("Simulação concluída. Consulta o ficheiro data/sync_result.csv." if dry_run
              else "Sync concluído na Amazon.", "success" if not dry_run else "info")

    except Exception as e:
        traceback.print_exc()
        flash(f"Erro no sync: {e}", "danger")

    return redirect(url_for("classify"))

@app.route("/download/<kind>")
def download(kind):
    filename = "produtos_processados.csv" if kind == "processed" else "sync_result.csv"
    path = os.path.join(DATA_DIR, filename)
    if not os.path.isfile(path):
        flash("Ficheiro não encontrado (gera primeiro).", "warning")
        return redirect(url_for("classify"))
    return send_file(path, as_attachment=True, download_name=filename)

if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", "5000"))
    app.run(host=host, port=port, debug=False)
