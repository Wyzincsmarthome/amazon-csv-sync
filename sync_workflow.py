#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import pandas as pd
from dotenv import load_dotenv

# Módulos do teu repositório
from csv_processor_visiotech import process_csv
from asin_resolver import resolve_asin
from amazon_client import patch_listings_item, put_listings_item


def _mask(v: str, keep: int = 3) -> str:
    """Mascara IDs em logs (segurança/logs limpos)."""
    if not v:
        return ""
    v = str(v)
    return v[:keep] + "***" if len(v) > keep else "***"

def _load_config(path: str = "config.json") -> dict:
    if not os.path.isfile(path):
        print(f"[ERRO] Faltou {path} no repositório.", file=sys.stderr)
        sys.exit(2)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _load_csv_with_processor(csv_path: str, cfg: dict) -> pd.DataFrame:
    """
    process_csv(csv_path, cfg) nas versões novas; em versões antigas pode ser process_csv(csv_path).
    Este wrapper tenta primeiro com cfg, e se falhar por assinatura, tenta sem cfg.
    """
    try:
        return process_csv(csv_path, cfg)  # assinatura nova
    except TypeError as e:
        # fallback para versões antigas
        print(f"[INFO] process_csv não aceitou cfg ({e}); a tentar sem cfg...")
        return process_csv(csv_path)

def main() -> None:
    load_dotenv(override=True)

    # Entradas e controlos
    csv_input = os.getenv("CSV_INPUT", "data/visiotech.csv")
    dry_run = os.getenv("DRY_RUN", "true").lower() in ("1", "true", "yes", "on")
    limit_env = os.getenv("LIMIT", "0")
    try:
        limit = int(limit_env) if str(limit_env).strip() else 0
    except Exception:
        limit = 0
    limit = limit if limit > 0 else None

    # Credenciais / IDs
    seller_id = os.getenv("SELLER_ID") or ""
    marketplace_id = os.getenv("MARKETPLACE_ID") or ""
    simulate = os.getenv("SPAPI_SIMULATE", "false").lower() in ("1", "true", "yes", "on")

    if not os.path.isfile(csv_input):
        print(f"[ERRO] CSV não encontrado: {csv_input}", file=sys.stderr)
        sys.exit(2)

    if not dry_run and (not seller_id or not marketplace_id):
        print("[ERRO] SELLER_ID e MARKETPLACE_ID são obrigatórios quando DRY_RUN=false.", file=sys.stderr)
        sys.exit(2)

    print(f"[INFO] Fonte CSV: {csv_input} | DRY_RUN={dry_run} | LIMIT={limit or '-'}")
    print(f"[INFO] SELLER_ID={_mask(seller_id)} | MARKETPLACE_ID={_mask(marketplace_id)} | SIMULATE={simulate}")

    # 1) Carregar configuração e CSV normalizado
    cfg = _load_config("config.json")
    df = _load_csv_with_processor(csv_input, cfg)

    if not isinstance(df, pd.DataFrame):
        print("[ERRO] csv_processor_visiotech.process_csv não devolveu DataFrame.", file=sys.stderr)
        sys.exit(2)

    if df.empty:
        print("[AVISO] DataFrame vazio após process_csv — nada para sincronizar.")
        # ainda assim escrevemos um ficheiro de saída vazio com cabeçalho
        os.makedirs("data", exist_ok=True)
        pd.DataFrame(columns=["sku", "ean", "stock", "final_price", "asin"]).to_csv(
            "data/sync_result.csv", index=False, encoding="utf-8"
        )
        print("OK -> data/sync_result.csv (vazio) gerado")
        return

    if limit:
        df = df.head(limit)

    # Higiene básica
    df = df.fillna("")
    # Colunas que esperamos (com tolerância a nomes)
    possible_price_cols = ["selling_price", "preview_price", "final_price", "price"]
    required_cols = ["sku", "ean", "stock"]
    missing_req = [c for c in required_cols if c not in df.columns]
    if missing_req:
        print(f"[ERRO] Faltam colunas obrigatórias no DF: {missing_req}", file=sys.stderr)
        print(f"[DEBUG] Colunas presentes: {list(df.columns)}", file=sys.stderr)
        sys.exit(2)

    # 2) Loop principal: resolver ASIN, decidir PATCH/PUT e (se DRY_RUN=false) enviar
    out_rows = []
    processed = 0
    for _, r in df.iterrows():
        sku = str(r.get("sku", "")).strip()
        ean = str(r.get("ean", "")).strip()
        brand = str(r.get("brand", "")).strip() if "brand" in df.columns else ""
        title = str(r.get("title", "")).strip() if "title" in df.columns else ""

        # stock
        try:
            stock = int(str(r.get("stock", "0")).strip() or 0)
        except Exception:
            stock = 0

        # preço
        price_val = 0.0
        for c in possible_price_cols:
            if c in df.columns and str(r.get(c, "")).strip() != "":
                try:
                    price_val = float(str(r.get(c)).replace(",", "."))
                    break
                except Exception:
                    continue

        # resolve ASIN (esta função no teu repo devolve dict)
        asin = ""
        try:
            asin_info = resolve_asin(sku=sku, name=title, brand=brand, ean=ean, seller_id=seller_id, simulate=simulate)
            if isinstance(asin_info, dict):
                asin = (asin_info.get("asin") or "").strip()
            elif isinstance(asin_info, str):
                asin = asin_info.strip()
        except Exception as e:
            print(f"[WARN] resolve_asin falhou para SKU={sku} EAN={ean}: {e}")

        # Decide ação
        if asin:
            action = "PATCH"  # atualizar listagem existente
        else:
            action = "PUT"    # criar listagem mínima

        # Execução / Simulação
        if dry_run:
            if action == "PATCH":
                print(f"[DRY] PATCH sku={sku} asin={asin} qty={stock} price={price_val:.2f}")
            else:
                print(f"[DRY] PUT   sku={sku} ean={ean}  qty={stock} price={price_val:.2f}")
        else:
            try:
                if action == "PATCH":
                    resp = patch_listings_item(
                        seller_id=seller_id,
                        sku=sku,
                        marketplace_id=marketplace_id,
                        price=price_val,
                        quantity=stock,
                    )
                else:
                    resp = put_listings_item(
                        seller_id=seller_id,
                        sku=sku,
                        marketplace_id=marketplace_id,
                        price=price_val,
                        quantity=stock,
                        ean=ean,
                    )
                # logging curto
                print(f"[OK] {action} {sku} -> {str(resp)[:200]}")
            except Exception as e:
                print(f"[ERRO] {action} {sku} falhou: {e}", file=sys.stderr)

        out_rows.append({
            "sku": sku,
            "ean": ean,
            "stock": stock,
            "final_price": price_val,
            "asin": asin if asin else "n/a",
        })
        processed += 1

    # 3) Guardar relatório
    os.makedirs("data", exist_ok=True)
    out_path = "data/sync_result.csv"
    pd.DataFrame(out_rows).to_csv(out_path, index=False, encoding="utf-8")
    print(f"OK -> {out_path} gerado")
    print(f"[INFO] Linhas processadas: {processed}")


if __name__ == "__main__":
    main()
