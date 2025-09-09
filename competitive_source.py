# competitive_source.py
# -*- coding: utf-8 -*-
"""
Ponto único para ires buscar o preço do concorrente (Buy Box ou menor preço):
- Aqui podes ligar SP-API (Product Pricing), Keepa, scraping, etc.
- Por agora é um stub e devolve None (sem concorrente).

Quando integrares:
- Preferência: SP-API getPricing (requires auth) para o ASIN.
- Se só tiveres EAN/SKU, converte para ASIN via catálogo e depois pricing.
"""

def get_competitor_price(sku: str | None = None,
                         ean: str | None = None,
                         asin: str | None = None,
                         brand: str | None = None) -> float | None:
    # TODO: implementar real: obter buy box price / lowest landed price
    # exemplo de retorno esperado: 160.09 (com IVA), ou None se desconhecido
    return None
