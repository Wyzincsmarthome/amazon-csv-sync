# app_flask.py
# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, jsonify, redirect, url_for, Response
import os, json, logging, pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from csv_processor_visiotech import process_csv, load_cfg
from amazon_client import AmazonClient
from product_identify import classify_products
from sync_workflow import plan_and_sync as _unused  # só para manter compat se existir
from inventory_sync import refresh_my_inventory

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "dev")
PORT = int(os.getenv("PORT", "5000"))

os.makedirs("data", exist_ok=True)
os.makedirs("uploads", exist_ok=True)
os.makedirs("logs", exist_ok=True)

SETTINGS_FILE = "data/settings.json"

def _get_simulate_flag():
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                s = json.load(f)
                return bool(s.get("simulate", True))
    except Exception:
        pass
    return os.getenv("SPAPI_SIMULATE", "true").lower() in ("1","true","yes","on")

def _set_simulate_flag(val: bool):
    data = {}
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    data["simulate"] = bool(val)
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _stats():
    cfg = load_cfg()
    stats = {
        "produtos_processados": 0,
        "marketplace_ativo": os.getenv("MARKETPLACE_ID","A1RKKUPIHCS9HS"),
        "ultima_sincronizacao": "-",
        "marcas": {},
        "simulate": _get_simulate_flag(),
    }
    p = "data/produtos_processados.csv"
    if os.path.exists(p):
        df = pd.read_csv(p)
        stats["produtos_processados"] = len(df)
        if "brand" in df.columns:
            stats["marcas"] = dict(df["brand"].value_counts().head(6))
    try:
        files = [f for f in os.listdir("data") if f.startswith("sync_") and f.endswith(".json")]
        files.sort(reverse=True)
        if files:
            with open(os.path.join("data", files[0]), "r", encoding="utf-8") as fh:
                last = json.load(fh)
            stats["ultima_sincronizacao"] = last.get("summary", {}).get("timestamp", "-")
    except Exception:
        pass
    return stats, cfg

@app.route("/")
def index():
    stats, _ = _stats()
    return render_template("index.html", stats=stats)

# -------- Upload + Review --------
@app.route("/upload_csv", methods=["POST"])
def upload_csv():
    f = request.files.get("csv_file")
    if not f or f.filename == "":
        return jsonify({"success": False, "error": "Nenhum ficheiro selecionado"}), 400
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join("uploads", f"visiotech_{ts}.csv")
    f.save(path)
    try:
        cfg = load_cfg()
        df = process_csv(path, cfg)
        data = {
            "total_produtos": int(len(df)),
            "preco_medio": float(df["selling_price"].mean()) if len(df) else 0.0,
            "marketplace": os.getenv("MARKETPLACE_ID","A1RKKUPIHCS9HS"),
            "vat_rate_usado": int(cfg.get("pricing_engine",{}).get("vat", 0)*100),
            "produtos_com_stock": int((df["stock"] > 0).sum()) if len(df) else 0
        }
        return jsonify({"success": True, "message":"CSV processado com sucesso.", "stats": data})
    except Exception as e:
        log.exception("Erro processamento CSV")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/review_data")
def review_data():
    p = "data/produtos_processados.csv"
    if not os.path.exists(p):
        return "Sem dados processados. Faça upload primeiro.", 400
    df = pd.read_csv(p, dtype=str).fillna("")
    return render_template("review.html", products=df.to_dict(orient="records"))

# -------- DEBUG CSV ----------
@app.route("/csv_debug")
def csv_debug():
    info = []
    if os.path.exists("data/_last_csv_read_info.txt"):
        with open("data/_last_csv_read_info.txt","r",encoding="utf-8") as f:
            info.append(f.read())
    if os.path.exists("data/produtos_processados.csv"):
        df = pd.read_csv("data/produtos_processados.csv")
        info.append("\n\nResumo custos/stock (primeiras 10 linhas):\n")
        cols = [c for c in ["sku","brand","cost","stock","preview_price","floor_price"] if c in df.columns]
        if cols:
            info.append(df[cols].head(10).to_string(index=False))
        info.append(f"\n\nTotal linhas: {len(df)}")
        if "cost" in df.columns:  info.append(f"\nCost zero %: {round((df['cost']<=0).mean()*100,2)}%")
        if "stock" in df.columns: info.append(f"\nStock zero %: {round((df['stock']<=0).mean()*100,2)}%")
    return Response("\n".join(info) or "Sem info.", mimetype="text/plain; charset=utf-8")

# -------- Teste API --------
@app.route("/test_api", methods=["POST"])
def test_api():
    simulate = _get_simulate_flag()
    cli = AmazonClient(simulate=simulate)
    try:
        _ = cli._get_token() if not simulate else "SIMULATED"
        return jsonify({"success": True, "message": f"Conectividade ({'simulado' if simulate else 'real'}) OK."})
    except Exception as e:
        return jsonify({"success": False, "error": f"Falha de conectividade: {e}"}), 500

# -------- Inventário Amazon --------
@app.route("/refresh_inventory", methods=["POST"])
def refresh_inventory_route():
    simulate = _get_simulate_flag()
    try:
        res = refresh_my_inventory(simulate=simulate)
        return jsonify({"success": True, "message": f"Inventário {'simulado' if simulate else 'real'} atualizado.", "result": res})
    except Exception as e:
        log.exception("Erro refresh_inventory")
        return jsonify({"success": False, "error": str(e)}), 500

# -------- Classificação --------
@app.route("/classify_run", methods=["POST","GET"])
def classify_run():
    simulate = _get_simulate_flag()
    seller_id = os.getenv("SELLER_ID")
    try:
        df = classify_products(simulate=simulate, seller_id=seller_id)
        return jsonify({"success": True, "message": f"Classificação concluída ({'simulado' if simulate else 'real'}).", "total": int(len(df))})
    except Exception as e:
        log.exception("Erro classificar")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/make_classified")
def make_classified():
    simulate = _get_simulate_flag()
    seller_id = os.getenv("SELLER_ID")
    try:
        classify_products(simulate=simulate, seller_id=seller_id)
    except Exception as e:
        return f"Erro a classificar: {e}", 500
    return redirect(url_for("classified"))

@app.route("/classified")
def classified():
    p = "data/produtos_classificados.csv"
    if not os.path.exists(p):
        return redirect(url_for("make_classified"))
    df = pd.read_csv(p, dtype=str).fillna("")
    simulate = _get_simulate_flag()

    # Converter asin_options (string JSON) -> lista python para preencher <select>
    rows = []
    import json as _json
    for _, r in df.iterrows():
        d = r.to_dict()
        raw = d.get("asin_options") or "[]"
        try:
            d["asin_options_list"] = _json.loads(raw)
        except Exception:
            d["asin_options_list"] = []
        rows.append(d)

    brands = sorted([b for b in df.get("brand", pd.Series([], dtype=str)).astype(str).unique() if b])
    return render_template("review_classified.html",
                           rows=rows,
                           simulate=simulate,
                           brands=brands)

@app.route("/approve_asin", methods=["POST"])
def approve_asin():
    data = request.get_json(force=True)
    sku = (data.get("sku") or "").strip()
    asin = (data.get("asin") or "").strip()
    if not sku or not asin:
        return jsonify({"success": False, "error":"sku e asin são obrigatórios"}), 400
    p = "data/produtos_classificados.csv"
    if not os.path.exists(p):
        return jsonify({"success": False, "error":"Ficheiro de classificados não encontrado."}), 400
    df = pd.read_csv(p, dtype=str).fillna("")
    mask = df["sku"].astype(str).str.strip().str.upper() == sku.upper()
    if not mask.any():
        return jsonify({"success": False, "error":"SKU não encontrado no classificado."}), 404
    df.loc[mask, "asin"] = asin
    df.loc[mask, "existence"] = "catalog_match"
    df.loc[mask, "action"] = "create_listing"
    df.to_csv(p, index=False)
    return jsonify({"success": True, "message":"ASIN aprovado e marcado para criar listing."})

@app.route("/bulk_approve", methods=["POST"])
def bulk_approve():
    import json as _json
    data = request.get_json(force=True)
    skus = data.get("skus", [])
    if not skus:
        return jsonify({"success": False, "error": "Sem SKUs."}), 400
    p = "data/produtos_classificados.csv"
    if not os.path.exists(p):
        return jsonify({"success": False, "error":"Ficheiro de classificados não encontrado."}), 400
    df = pd.read_csv(p, dtype=str).fillna("")
    approved = []
    for sku in skus:
        sku_norm = str(sku).strip()
        mask = df["sku"].astype(str).str.strip().str.upper() == sku_norm.upper()
        if not mask.any(): continue
        row = df[mask].iloc[0].to_dict()
        if row.get("existence") != "catalog_ambiguous": continue
        brand = (row.get("brand") or "").strip().casefold()
        options_json = row.get("asin_options") or "[]"
        try: options = _json.loads(options_json)
        except Exception: options = []
        pick = None
        for o in options:
            if str(o.get("brand","")).strip().casefold() == brand and o.get("asin"):
                pick = o["asin"]; break
        if not pick and options: pick = options[0].get("asin")
        if pick:
            df.loc[mask, "asin"] = pick
            df.loc[mask, "existence"] = "catalog_match"
            df.loc[mask, "action"] = "create_listing"
            approved.append({"sku": sku_norm, "asin": pick})
    df.to_csv(p, index=False)
    return jsonify({"success": True, "approved": len(approved), "items": approved})

# -------- Ofertas (comparador) --------
@app.route("/offers_min")
def offers_min():
    asin = (request.args.get("asin") or "").strip()
    if not asin:
        return jsonify({"success": False, "error":"asin é obrigatório"}), 400
    simulate = _get_simulate_flag()
    cli = AmazonClient(simulate=simulate)
    try:
        offers = cli.get_listing_offers(asin)
        if not offers:
            return jsonify({"success": True, "asin": asin, "min_price": None, "offers": []})
        landed = [float(o.get("LandedPrice", 0.0) or 0.0) for o in offers if o.get("LandedPrice") is not None]
        minp = min(landed) if landed else None
        top = sorted(offers, key=lambda x: x.get("LandedPrice") or 0.0)[:5]
        return jsonify({"success": True, "asin": asin, "min_price": minp, "offers": top})
    except Exception as e:
        log.exception("Erro offers_min")
        return jsonify({"success": False, "error": str(e)}), 500

# -------- Sincronização a partir do classificado (usa concorrência) --------
@app.route("/sync_from_classified", methods=["POST"])
def sync_from_classified():
    """
    Calcula preço recomendado = max(floor, min(LandedPrice)-0.01), e envia feeds
    de PREÇO e INVENTÁRIO. Em simulação, só grava o resumo.
    """
    data = request.get_json(force=True)
    rows_sel = data.get("rows", [])
    if not rows_sel:
        return jsonify({"success": False, "error": "Sem linhas selecionadas."}), 400

    simulate = _get_simulate_flag()
    cfg = load_cfg()
    cli = AmazonClient(simulate=simulate)

    # ler processados/classificados para enriquecer
    df_proc = pd.read_csv("data/produtos_processados.csv", dtype=str).fillna("") if os.path.exists("data/produtos_processados.csv") else pd.DataFrame()
    if "floor_price" in df_proc.columns:
        df_proc["floor_price"] = df_proc["floor_price"].astype(float)
    if "stock" in df_proc.columns:
        try: df_proc["stock"] = df_proc["stock"].astype(int)
        except: df_proc["stock"] = 0

    # map sku->(floor,stock)
    floors = {str(r["sku"]).strip().upper(): float(r.get("floor_price", 0) or 0.0) for _, r in df_proc.iterrows()} if not df_proc.empty else {}
    stocks = {str(r["sku"]).strip().upper(): int(float(r.get("stock", 0) or 0)) for _, r in df_proc.iterrows()} if not df_proc.empty else {}

    # calcular preços finais com base na concorrência
    from math import isfinite
    undercut = float(cfg.get("pricing_engine",{}).get("undercut_step", 0.01))
    items = []
    for r in rows_sel:
        sku  = (r.get("sku") or "").strip()
        asin = (r.get("asin") or "").strip()
        if not sku or not asin:
            continue
        key = sku.upper()
        floor = floors.get(key, 0.0)
        stock = stocks.get(key, 0)
        min_comp = None
        try:
            offers = cli.get_listing_offers(asin)
            if offers:
                landed = [float(o.get("LandedPrice", 0.0) or 0.0) for o in offers if o.get("LandedPrice") is not None]
                min_comp = min(landed) if landed else None
        except Exception as e:
            log.warning("Falha a obter ofertas para %s (%s): %s", sku, asin, e)
        if min_comp and isfinite(min_comp) and min_comp > 0:
            rec = round(min_comp - undercut + 1e-9, 2)
            final = rec if rec >= floor else floor
        else:
            final = round(floor + 1e-9, 2)
        items.append({"sku": sku, "asin": asin, "selling_price": final, "stock": stock})

    # construir dataframes
    if not items:
        return jsonify({"success": False, "error": "Nenhum item válido para sync."}), 400
    df_price = pd.DataFrame([{"sku": it["sku"], "selling_price": it["selling_price"]} for it in items])
    df_stock = pd.DataFrame([{"sku": it["sku"], "stock": it["stock"]} for it in items])

    # SIMULAÇÃO -> grava resumo
    ts = datetime.utcnow().isoformat()
    sync_file = f"data/sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    if simulate:
        summary = {
            "feeds": {
                "pricing":  {"feedId": None, "status": "SIMULATED"},
                "inventory":{"feedId": None, "status": "SIMULATED"},
            },
            "offers_created": 0,
            "prices_sent": int(len(df_price)),
            "stock_sent": int(len(df_stock)),
            "timestamp": ts
        }
        with open(sync_file,"w",encoding="utf-8") as f:
            json.dump({"success":True, "summary": summary}, f, ensure_ascii=False, indent=2)
        return jsonify({"success": True, "message": f"Simulação concluída ({len(items)} itens).", "summary": summary})

    # REAL -> submete feeds (usa helpers do AmazonClient que já usaste)
    try:
        # PRICING
        pricing_tsv = cli.feed_pricing_tsv(df_price)
        doc_p = cli._create_feed_document()
        cli._upload_feed_document(doc_p, pricing_tsv.encode("utf-8"))
        feed_p = cli._create_feed("POST_PRODUCT_PRICING_DATA", doc_p["feedDocumentId"])

        # INVENTORY
        inv_tsv = cli.feed_inventory_tsv(df_stock)
        doc_i = cli._create_feed_document()
        cli._upload_feed_document(doc_i, inv_tsv.encode("utf-8"))
        feed_i = cli._create_feed("POST_INVENTORY_AVAILABILITY_DATA", doc_i["feedDocumentId"])

        summary = {
            "feeds": {
                "pricing":  {"feedId": str(feed_p.get("feedId")), "status": "SUBMITTED"},
                "inventory":{"feedId": str(feed_i.get("feedId")), "status": "SUBMITTED"},
            },
            "offers_created": 0,
            "prices_sent": int(len(df_price)),
            "stock_sent": int(len(df_stock)),
            "timestamp": ts
        }
        with open(sync_file,"w",encoding="utf-8") as f:
            json.dump({"success":True, "summary": summary}, f, ensure_ascii=False, indent=2)
        return jsonify({"success": True, "message": f"Real: {len(items)} itens submetidos.", "summary": summary})
    except Exception as e:
        log.exception("Erro a submeter feeds")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/toggle_simulate", methods=["GET","POST"])
def toggle_simulate():
    if request.method == "GET":
        on = (request.args.get("on","").lower() in ("1","true","yes","on"))
        _set_simulate_flag(on)
        return jsonify({"success": True, "simulate": on})
    data = request.get_json(force=True)
    val = bool(data.get("simulate", True))
    _set_simulate_flag(val)
    return jsonify({"success": True, "simulate": val})

# util
@app.route("/last_sync")
def last_sync():
    try:
        files = [f for f in os.listdir("data") if f.startswith("sync_") and f.endswith(".json")]
        if not files:
            return jsonify({"success": False, "error": "Ainda não existe nenhum sync_*.json."}), 404
        files.sort(reverse=True)
        p = os.path.join("data", files[0])
        with open(p, "r", encoding="utf-8") as f:
            j = json.load(f)
        return jsonify({"success": True, "file": files[0], "content": j})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=PORT)
