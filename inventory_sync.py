# inventory_sync.py
# -*- coding: utf-8 -*-
import logging
from amazon_reports import fetch_my_inventory

log = logging.getLogger(__name__)

def refresh_my_inventory(simulate: bool = False) -> dict:
    """
    Faz o download do inventário atual (GET_MERCHANT_LISTINGS_ALL_DATA)
    e grava em data/my_inventory.csv. Devolve {'file':..., 'rows':N}
    """
    path, rows = fetch_my_inventory(simulate=simulate)
    return {"file": path, "rows": int(rows)}
