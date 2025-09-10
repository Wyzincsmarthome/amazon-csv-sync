import os
import pandas as pd
from typing import Dict, Tuple
from dotenv import load_dotenv

from csv_processor_visiotech import process_csv, load_cfg
from pricing_engine import calc_final_price
from asin_resolver import resolve_asin
from amazon_client_updated import patch_listings_item, put_listings_item


load_dotenv()

# Caminho para o CSV de entrada e flag dry-run
CSV_INPUT: str = os.getenv("CSV_INPUT", "data/visiotech.csv")
DRY_RUN: bool = os.getenv("DRY_RUN", "true").lower() in ("1", "true", "yes", "on")


def _attributes_minimos_for_creation(row: Dict) -> Tuple[str, Dict]:
    """
    Gera um par (product_type, attributes) mínimo para criar uma listagem
    quando ainda não existe ASIN associado. Utiliza heurísticas simples baseadas
    na categoria do produto. Na fase seguinte, poderá ser integrada a API
    ``productTypeDefinitions`` para obter schemas completos.

    Args:
        row: Dicionário representando uma linha do DataFrame, já contendo
            ``final_price`` e ``stock``.

    Returns:
        Tuple contendo o ``product_type`` e um dicionário ``attributes`` com
        campos mínimos para criação via PUT.
    """
    title = row.get("title", "")
    brand = row.get("brand", "")
    ean = row.get("ean", "") or ""
    qty = int(row.get("stock", 0) or 0)
    price = float(row.get("final_price", 0.0))

    # Heurística para determinar o tipo de produto
    category = str(row.get("category", "")).lower()
    if "camera" in category:
        ptype = "camera"
    elif "lock" in category:
        ptype = "lock"
    else:
        ptype = "product"

    attributes = {
        "brand": brand,
        "item_name": title,
        "external_product_id": ean,
        "external_product_id_type": "EAN",
        "purchasable_offer": {
            "currency": "EUR",
            "our_price": [
                {
                    "schedule": [
                        {
                            "value_with_tax": {
                                "amount": f"{price:.2f}",
                                "currency": "EUR",
                            }
                        }
                    ]
                }
            ],
        },
        "fulfillment_availability": [
            {
                "fulfillment_channel_code": "DEFAULT",
                "quantity": qty,
            }
        ],
    }
    return ptype, attributes


def main() -> None:
    print(f"Fonte CSV: {CSV_INPUT} | DRY_RUN={DRY_RUN}")
    # Carregar configuração (config.json)
    cfg = load_cfg()
    # Processar CSV com a configuração
    df = process_csv(CSV_INPUT, cfg).fillna("")

    out_rows = []
    for _, r in df.iterrows():
        sku = str(r.get("sku", "")).strip()
        ean = str(r.get("ean", "")).strip()
        stock = int(str(r.get("stock", 0)))
        cost_val = r.get("cost", 0.0)
        try:
            cost = float(cost_val)
        except Exception:
            cost = 0.0

        # Calcular preço final com IVA segundo a tua configuração
        price_dict = calc_final_price(cost=cost, competitor_price=None, cfg=cfg)
        final_price = float(price_dict.get("final_price", 0.0))

        title = str(r.get("title", "")).strip()
        brand = str(r.get("brand", "")).strip()
        category = str(r.get("category", "")).strip()

        # Construir dicionário para criação (inclui final_price)
        row_dict = r.to_dict()
        row_dict["final_price"] = final_price

        # Resolver ASIN (pode devolver listed/catalog_match/catalog_ambiguous/not_found)
        asin = ""
        try:
            res = resolve_asin(
                sku=sku,
                name=title,
                brand=brand,
                ean=ean or None,
                seller_id=None,
                simulate=None,
            )
            asin = str(res.get("asin", "") or "").strip()
        except Exception as e:
            print(f"[WARN] Falha a resolver ASIN para {sku}/{ean}: {e}")

        # Actualização ou criação
        if asin:
            if DRY_RUN:
                print(
                    f"[DRY] PATCH SKU={sku} ASIN={asin} stock={stock} price={final_price:.2f}"
                )
            else:
                try:
                    patch_listings_item(sku=sku, quantity=stock, price_with_tax_eur=final_price)
                except Exception as exc:
                    print(
                        f"[ERROR] PATCH falhou para {sku} ASIN={asin}: {exc}"
                    )
        else:
            ptype, attrs = _attributes_minimos_for_creation(row_dict)
            if DRY_RUN:
                print(
                    f"[DRY] PUT SKU={sku} ptype={ptype} EAN={ean} stock={stock} price={final_price:.2f}"
                )
            else:
                try:
                    put_listings_item(sku=sku, product_type=ptype, attributes=attrs)
                except Exception as exc:
                    print(
                        f"[ERROR] PUT falhou para {sku} EAN={ean}: {exc}"
                    )

        out_rows.append(
            {
                "sku": sku,
                "ean": ean,
                "stock": stock,
                "final_price": final_price,
                "asin": asin or "n/a",
            }
        )

    # Guardar o resultado para referência
    os.makedirs("data", exist_ok=True)
    pd.DataFrame(out_rows).to_csv("data/sync_result.csv", index=False, encoding="utf-8")
    print("OK -> data/sync_result.csv gerado")


if __name__ == "__main__":
    main()
