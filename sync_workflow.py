# sync_workflow.py
# Pipeline: lê CSV Visiotech -> calcula preço -> tenta ASIN por EAN -> PATCH (stock/preço)
# Se não houver ASIN/SKU, faz PUT com productType+attributes mínimos (fase 1).
# Mantém o uso de SELLER_ID, MARKETPLACE_ID e pricing_engine/config existentes.

import os
import pandas as pd
from decimal import Decimal
from dotenv import load_dotenv

from csv_processor_visiotech import process_csv  # usa tua função de normalização atual
from pricing_engine import calc_final_price      # usa tua regra existente (com IVA + frete se já aplicas)
from asin_resolver import resolve_asin           # mantém tua lógica atual
from amazon_client import patch_listings_item, put_listings_item

load_dotenv()

CSV_INPUT = os.getenv("CSV_INPUT", "data/visiotech.csv")
DRY_RUN   = os.getenv("DRY_RUN", "true").lower() == "true"

def _attributes_minimos_for_creation(row: dict) -> tuple[str, dict]:
    """
    Fase 1: criação simples. Define productType generico e atributos essenciais.
    Na fase 2 podemos puxar Product Type Definitions para preencher atributos completos por categoria.
    """
    # Usa heurística simples: se categoria contém 'Camera' -> 'camera', se 'Lock' -> 'lock', senão 'product'
    title = row["title"]
    brand = row["brand"]
    ean   = row.get("ean") or ""
    qty   = int(row.get("stock") or 0)
    price = float(row["final_price"])

    cat = (row.get("category") or "").lower()
    if "camera" in cat: ptype = "camera"
    elif "lock" in cat: ptype = "lock"
    else: ptype = "product"

    attributes = {
        "brand": brand,
        "item_name": title,
        "external_product_id": ean,
        "external_product_id_type": "EAN",
        "purchasable_offer": {
            "currency":"EUR",
            "our_price":[{"schedule":[{"value_with_tax":{"amount": f"{price:.2f}", "currency":"EUR"}}]}]
        },
        "fulfillment_availability":[{"fulfillment_channel_code":"DEFAULT","quantity": qty}]
    }
    return ptype, attributes

def main():
    print(f"Fonte CSV: {CSV_INPUT}  | DRY_RUN={DRY_RUN}")
    df = process_csv(CSV_INPUT)  # deve devolver colunas: sku, ean, brand, title, category, cost, stock...
    df = df.fillna("")

    out_rows = []
    for _, r in df.iterrows():
        sku   = str(r["sku"]).strip()
        ean   = str(r.get("ean","")).strip()
        stock = int(str(r.get("stock","0")))
        cost  = Decimal(str(r.get("cost","0")))
        price = float(calc_final_price(cost))  # respeita a tua engine atual
        title = (r.get("title") or "").strip()
        brand = (r.get("brand") or "").strip()
        category = (r.get("category") or "").strip()

        # Enriquecer linha com final_price para criação PUT
        row = {**r.to_dict(), "final_price": price}

        asin = ""
        if ean:
            try:
                asin = resolve_asin(ean=ean, name=title, brand=brand) or ""
            except Exception as e:
                print(f"[WARN] Falha a resolver ASIN para {sku}/{ean}: {e}")

        if asin:
            # Atualiza stock/preço via PATCH
            if DRY_RUN:
                print(f"[DRY] PATCH SKU={sku} ASIN={asin} stock={stock} price={price:.2f}")
            else:
                patch_listings_item(sku=sku, quantity=stock, price_with_tax_eur=price)
        else:
            # Cria listagem via PUT (Fase 1: atributos mínimos + heurística de productType)
            ptype, attrs = _attributes_minimos_for_creation(row)
            if DRY_RUN:
                print(f"[DRY] PUT SKU={sku} ptype={ptype} EAN={ean} stock={stock} price={price:.2f}")
            else:
                put_listings_item(sku=sku, product_type=ptype, attributes=attrs)

        out_rows.append({"sku":sku, "ean":ean, "stock":stock, "final_price":price, "asin":asin or "n/a"})

    pd.DataFrame(out_rows).to_csv("data/sync_result.csv", index=False, encoding="utf-8")
    print("OK -> data/sync_result.csv gerado")

if __name__ == "__main__":
    main()
