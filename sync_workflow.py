#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
from dotenv import load_dotenv

# Do teu repo:
# - csv_processor_visiotech expõe process_csv (não existe load_visiotech_csv)
# - asin_resolver.resolve_asin devolve dict {status, asin, ...}
from csv_processor_visiotech import process_csv
from asin_resolver import resolve_asin

# Só serão usados quando DRY_RUN=false
from amazon_client import patch_listings_item, put_listings_item


def main() -> None:
    load_dotenv()

    csv_input = os.getenv("CSV_INPUT", "data/visiotech.csv")
    dry_run = os.getenv("DRY_RUN", "true").lower() in ("1", "true", "yes", "on")
    limit = int(os.getenv("LIMIT", "0")) or None

    seller_id = os.getenv("SELLER_ID") or ""
    marketplace_id = os.getenv("MARKETPLACE_ID") or ""

    if not os.path.isfile(csv_input):
        print(f"[ERRO] CSV não encontrado: {csv_input}", file=sys.stderr)
        sys.exit(2)

    print(f"[INFO] Fonte CSV: {csv_input} | DRY_RUN={dry_run} | LIMIT={limit or '-'}")
    print(f"[INFO] SELLER_ID={seller_id} | MARKETPLACE_ID={marketplace_id}")

    # 1) Ler e normalizar CSV do fornecedor
    # process_csv escreve data/_last_csv_read_info.txt com diagnóstico e devolve DF
    df = process_csv(csv_input)  # contém: sku, ean, brand, title, category, cost, stock, selling_price, ...
    if limit:
        df = df.head(limit)
    df = df.fillna("")

    out_rows = []

    for _, r in df.iterrows():
        sku = str(r.get("sku", "")).strip()
        ean = str(r.get("ean", "")).strip()
        stock = int(str(r.get("stock", "0")).strip() or 0)
        title = str(r.get("title", "")).strip()
        brand = str(r.get("brand", "")).strip()

        # Usa preço já calculado pelo process_csv (selling_price), com fallback para preview_price
        price = r.get("selling_price", r.get("preview_price", 0))
        try:
            price = float(price)
        except Exception:
            price = 0.0

        # 2) Resolver ASIN (usa sessão simulada se SPAPI_SIMULATE=true)
        asin = ""
        try:
            asin_info = resolve_asin(sku=sku, name=title, brand=brand, ean=ean, seller_id=seller_id)
            asin = (asin_info or {}).get("asin") or ""
        except Exception as e:
            print(f"[WARN] resolve_asin falhou para SKU={sku} EAN={ean}: {e}")

        # 3) PATCH (se já houver ASIN) ou PUT (criação mínima)
        if asin:
            action = "PATCH"
            if dry_run:
                print(f"[DRY] PATCH sku={sku} asin={asin} qty={stock} price={price:.2f}")
            else:
                # A tua amazon_client já implementa estes helpers
                patch_listings_item(
                    seller_id=seller_id,
                    sku=sku,
                    marketplace_id=marketplace_id,
                    price=price,
                    quantity=stock,
                )
        else:
            action = "PUT"
            if dry_run:
                print(f"[DRY] PUT sku={sku} ean={ean} qty={stock} price={price:.2f}")
            else:
                # Versão mínima (a tua amazon_client aceita ean/price/quantity)
                put_listings_item(
                    seller_id=seller_id,
                    sku=sku,
                    marketplace_id=marketplace_id,
                    price=price,
                    quantity=stock,
                    ean=ean,
                )

        out_rows.append({
            "sku": sku,
            "ean": ean,
            "stock": stock,
            "final_price": price,
            "asin": asin or "n/a",
        })

    # 4) Guardar relatório para conferência
    os.makedirs("data", exist_ok=True)
    pd.DataFrame(out_rows).to_csv("data/sync_result.csv", index=False, encoding="utf-8")
    print("OK -> data/sync_result.csv gerado")


if __name__ == "__main__":
    main()
