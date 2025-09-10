# product_identify.py
# -*- coding: utf-8 -*-
import os
import json
import logging
import pandas as pd

from asin_resolver import resolve_asin
from pricing_engine import calc_final_price
from csv_processor_visiotech import load_cfg

log = logging.getLogger(__name__)

def _load_my_inventory() -> pd.DataFrame:
    p = "data/my_inventory.csv"
    if os.path.exists(p):
        df = pd.read_csv(p, dtype=str).fillna("")
        if "asin" not in df.columns: df["asin"] = ""
        if "seller_sku" not in df.columns: df["seller_sku"] = ""
        return df
    return pd.DataFrame(columns=["asin", "seller_sku", "price", "quantity", "condition", "status"])

def _ensure_prices(df: pd.DataFrame) -> pd.DataFrame:
    need_preview = "preview_price" not in df.columns
    need_floor   = "floor_price" not in df.columns
    if not (need_preview or need_floor):
        return df
    cfg = load_cfg()
    if "cost" not in df.columns: df["cost"] = 0.0

    def _f(x):
        try:
            return float(str(x).replace(",", "."))
        except:
            return 0.0

    previews, floors = [], []
    for c in df["cost"].map(_f):
        out = calc_final_price(cost=c, competitor_price=None, cfg=cfg)
        previews.append(out["final_price"])
        floors.append(out["floor_price"])
    if need_preview:
        df["preview_price"] = previews
    if need_floor:
        df["floor_price"] = floors
    return df

def classify_products(input_csv="data/produtos_processados.csv",
                      output_csv="data/produtos_classificados.csv",
                      seller_id=None, simulate=True) -> pd.DataFrame:
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Falta o ficheiro {input_csv}. Faça upload/processamento primeiro.")
    
    df = pd.read_csv(input_csv, dtype=str).fillna("")
    for c in ("sku", "ean", "brand", "title", "category"):
        if c not in df.columns:
            df[c] = ""
    
    df = _ensure_prices(df)

    inv = _load_my_inventory()
    asins_meus = set(inv["asin"].astype(str).str.strip().str.upper().tolist())
    asin2sku = {str(r["asin"]).strip().upper(): str(r["seller_sku"]) for _, r in inv.iterrows()}

    out_rows = []
    for _, row in df.iterrows():
        sku   = str(row.get("sku", "")).strip()
        ean   = str(row.get("ean", "")).strip()
        brand = str(row.get("brand", "")).strip()
        title = str(row.get("title", "")).strip() or sku

        res = resolve_asin(sku=sku, name=title, brand=brand, ean=ean or None,
                           seller_id=seller_id, simulate=simulate)
        
        status = res.get("status", "not_found")
        asin   = str(res.get("asin", "")).strip().upper()
        score  = float(res.get("score", 0.0))
        cands  = res.get("candidates", [])

        my_sku_for_asin = ""
        if asin and asin in asins_meus:
            status = "listed"
            my_sku_for_asin = asin2sku.get(asin, "")

        if status == "listed":
            action = "update_price_stock"
        elif status == "catalog_match":
            action = "create_listing"
        elif status == "catalog_ambiguous":
            action = "review"
        else:
            action = "create_product"

        out = row.to_dict()
        out.update({
            "existence": status,
            "asin": asin,
            "match_score": round(score, 2),
            "action": action,
            "my_seller_sku": my_sku_for_asin,
            "asin_options": json.dumps(cands, ensure_ascii=False)
        })
        out_rows.append(out)

    out_df = pd.DataFrame(out_rows)
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    out_df.to_csv(output_csv, index=False)
    log.info("Classificados %d produtos -> %s", len(out_df), output_csv)
    return out_df
