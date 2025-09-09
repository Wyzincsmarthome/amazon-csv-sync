# sync_workflow.py
# -*- coding: utf-8 -*-
import os, json, logging
import pandas as pd
from datetime import datetime
from amazon_client import AmazonClient
from csv_processor_visiotech import load_cfg

log = logging.getLogger(__name__)

def _load_processed():
    p = "data/produtos_processados.csv"
    if not os.path.exists(p):
        raise FileNotFoundError("Falta data/produtos_processados.csv (faz upload/processamento primeiro).")
    return pd.read_csv(p)

def _load_classified():
    p = "data/produtos_classificados.csv"
    if not os.path.exists(p):
        raise FileNotFoundError("Falta data/produtos_classificados.csv (classifica primeiro).")
    return pd.read_csv(p, dtype=str).fillna("")

def plan_and_sync(selected_rows: list[dict], simulate: bool = True) -> dict:
    df_proc = _load_processed()
    df_cls  = _load_classified()

    sel = pd.DataFrame(selected_rows)
    sel["sku"] = sel["sku"].astype(str)

    merged = sel.merge(df_proc, on="sku", how="left", suffixes=("","_p"))
    merged = merged.merge(
        df_cls[["sku","asin","existence"]].rename(columns={"asin":"asin_cls","existence":"existence_cls"}),
        on="sku", how="left"
    )
    merged["asin"] = merged.apply(lambda r: r["asin"] if str(r.get("asin","")).strip() else str(r.get("asin_cls","")).strip(), axis=1)
    merged["existence"] = merged.apply(lambda r: r["existence"] if str(r.get("existence","")).strip() else str(r.get("existence_cls","")).strip(), axis=1)

    to_offer  = merged[(merged["existence"]=="catalog_match") & (merged["asin"].astype(str).str.len() > 0)]
    to_update = merged[(merged["existence"].isin(["listed","catalog_match"]))]

    offers_created = 0
    prices_sent = 0
    stock_sent  = 0

    feeds_info = {}

    cfg = load_cfg()
    client = AmazonClient(simulate=simulate)

    if simulate:
        offers_created = int(len(to_offer))
        prices_sent    = int(len(to_update))
        stock_sent     = int(len(to_update))
    else:
        # 1) Offers (listings)
        if len(to_offer):
            offers_tsv = _build_offers_tsv(to_offer)
            feed_id = client.submit_tsv("POST_FLAT_FILE_LISTINGS_DATA", offers_tsv)
            st = client.wait_feed(feed_id)
            report_file = client.save_processing_report(feed_id, "POST_FLAT_FILE_LISTINGS_DATA")
            feeds_info["offers"] = {"feedId": feed_id, "status": st.get("processingStatus"), "report": report_file}
            offers_created = len(to_offer)

        # 2) Pricing
        if len(to_update):
            pr_tsv = client.feed_pricing_tsv(to_update.rename(columns={"selling_price":"price"}))
            feed_id = client.submit_tsv("POST_PRODUCT_PRICING_DATA", pr_tsv)
            st = client.wait_feed(feed_id)
            report_file = client.save_processing_report(feed_id, "POST_PRODUCT_PRICING_DATA")
            feeds_info["pricing"] = {"feedId": feed_id, "status": st.get("processingStatus"), "report": report_file}
            prices_sent = len(to_update)

        # 3) Inventory
        if len(to_update):
            inv_tsv = client.feed_inventory_tsv(to_update.rename(columns={"stock":"quantity"}))
            feed_id = client.submit_tsv("POST_INVENTORY_AVAILABILITY_DATA", inv_tsv)
            st = client.wait_feed(feed_id)
            report_file = client.save_processing_report(feed_id, "POST_INVENTORY_AVAILABILITY_DATA")
            feeds_info["inventory"] = {"feedId": feed_id, "status": st.get("processingStatus"), "report": report_file}
            stock_sent = len(to_update)

    summary = {
        "timestamp": datetime.utcnow().isoformat(),
        "offers_created": int(offers_created),
        "prices_sent": int(prices_sent),
        "stock_sent": int(stock_sent),
        "feeds": feeds_info
    }
    os.makedirs("data", exist_ok=True)
    with open(f"data/sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json","w",encoding="utf-8") as f:
        json.dump({"status":"dry_run" if simulate else "success", "summary": summary}, f, ensure_ascii=False, indent=2)
    return summary

def _build_offers_tsv(df: pd.DataFrame) -> str:
    # Campos compatíveis para POST_FLAT_FILE_LISTINGS_DATA:
    # sku, product-id (ASIN), product-id-type (1=ASIN), price, quantity, add-delete, condition-type
    headers = ["sku","product-id","product-id-type","price","quantity","add-delete","condition-type"]
    lines = ["\t".join(headers)]
    for _, r in df.iterrows():
        sku = str(r.get("sku","")).strip()
        price = str(r.get("selling_price","")).strip() or str(r.get("price","")).strip() or "0.00"
        qty = str(int(float(r.get("stock",0) or 0)))
        asin = str(r.get("asin","")).strip()
        row = [sku, asin, "1", price, qty, "a", "New"]
        lines.append("\t".join(row))
    return "\n".join(lines)
