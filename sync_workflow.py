#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import argparse
from typing import Dict, Any, List

from dotenv import load_dotenv

# módulos do projeto
# csv_processor_visiotech: lê CSV do fornecedor, normaliza e devolve lista de dicts
# pricing_engine: calcula preço final com base no config.json
# amazon_client: envia PATCH/PUT para Listings Items API
from csv_processor_visiotech import load_visiotech_csv
from pricing_engine import calc_final_price
from amazon_client import patch_listings_item, put_listings_item

# ---------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync catálogo para Amazon a partir do CSV do fornecedor (Visiotech).")
    p.add_argument("--csv-input", default=os.getenv("CSV_INPUT", "data/visiotech.csv"),
                   help="Caminho para o CSV de entrada (default: data/visiotech.csv ou $CSV_INPUT)")
    p.add_argument("--limit", type=int, default=int(os.getenv("LIMIT", "0")),
                   help="Limitar o número de SKUs processados (0 = sem limite)")
    return p.parse_args()

def ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def write_sync_result(rows: List[Dict[str, Any]], dest_path: str) -> None:
    ensure_parent_dir(dest_path)
    if not rows:
        with open(dest_path, "w", newline="", encoding="utf-8") as f:
            f.write("sku,ean,stock,final_price,asin,action,status,message\n")
        return
    fieldnames = ["sku", "ean", "stock", "final_price", "asin", "action", "status", "message"]
    with open(dest_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def resolve_asin(item: Dict[str, Any]) -> str:
    """
    Estratégia simples: usar EAN/GTIN quando disponível.
    Se já vier ASIN do processador, respeitar. Caso contrário "n/a".
    A tua resolução detalhada pode estar noutro módulo; ajusta aqui se necessário.
    """
    asin = (item.get("asin") or "").strip()
    if asin:
        return asin
    # se o teu fluxo mapear EAN->ASIN noutro passo, invoca-o aqui
    return "n/a"

# ---------------------------------------------------------------------

def main() -> int:
    load_dotenv(override=True)

    args = parse_args()
    csv_input = args.csv_input
    limit = args.limit if args.limit and args.limit > 0 else None

    dry_run = (os.getenv("DRY_RUN", "true").lower() == "true")
    marketplace_id = os.getenv("MARKETPLACE_ID")
    seller_id = os.getenv("SELLER_ID")

    if not os.path.isfile(csv_input):
        print(f"[ERRO] CSV não encontrado: {csv_input}", file=sys.stderr)
        return 2

    print(f"[INFO] DRY_RUN={dry_run} | CSV={csv_input} | LIMIT={limit or '-'}")
    print(f"[INFO] MARKETPLACE_ID={marketplace_id} | SELLER_ID={seller_id}")

    # 1) Ler e normalizar CSV do fornecedor
    items = load_visiotech_csv(csv_input)
    if limit:
        items = items[:limit]
    print(f"[INFO] Linhas carregadas: {len(items)}")

    # 2) Calcular preço final e preparar linhas de sync
    results: List[Dict[str, Any]] = []
    for item in items:
        # calc preço final
        pricing = calc_final_price(item)
        final_price = pricing.get("final_price")
        sku = (item.get("sku") or "").strip()
        ean = (item.get("ean") or "").strip()
        stock = int(item.get("stock") or 0)

        asin = resolve_asin(item)

        # decidir acção
        if asin and asin != "n/a":
            action = "PATCH"   # atualizar listagem existente
        else:
            action = "PUT"     # criar listagem mínima

        results.append({
            "sku": sku,
            "ean": ean,
            "stock": stock,
            "final_price": final_price,
            "asin": asin,
            "action": action,
            "status": "pending",
            "message": ""
        })

    # 3) Enviar (ou simular) para a Amazon
    for r in results:
        sku = r["sku"]
        asin = r["asin"]
        stock = r["stock"]
        price = r["final_price"]

        try:
            if dry_run:
                r["status"] = "DRY_RUN"
                r["message"] = f"Simulação de {r['action']} sku={sku} asin={asin} price={price} stock={stock}"
            else:
                if r["action"] == "PATCH":
                    resp = patch_listings_item(
                        seller_id=seller_id,
                        sku=sku,
                        marketplace_id=marketplace_id,
                        price=price,
                        quantity=stock
                    )
                else:
                    resp = put_listings_item(
                        seller_id=seller_id,
                        sku=sku,
                        marketplace_id=marketplace_id,
                        price=price,
                        quantity=stock,
                        ean=r["ean"]
                    )
                r["status"] = "OK"
                r["message"] = str(resp)[:500]
        except Exception as e:
            r["status"] = "ERROR"
            r["message"] = str(e)[:500]

    # 4) Guardar relatório
    out_csv = "data/sync_result.csv"
    write_sync_result(results, out_csv)
    print(f"[INFO] Resultado escrito em: {out_csv}")

    # resumo
    ok = sum(1 for r in results if r["status"] in ("OK", "DRY_RUN"))
    err = sum(1 for r in results if r["status"] == "ERROR")
    print(f"[INFO] Concluído. OK/DRY={ok} | ERROS={err}")

    return 0 if err == 0 else 1

# ---------------------------------------------------------------------

if __name__ == "__main__":
    sys.exit(main())
