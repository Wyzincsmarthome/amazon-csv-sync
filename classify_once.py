# classify_once.py
# -*- coding: utf-8 -*-
import os
from product_identify import classify_products

# Força modo REAL aqui (muda para True se quiseres simulado)
df = classify_products(simulate=False, seller_id=os.getenv("SELLER_ID"))
print("OK — 'data/produtos_classificados.csv' criado com", len(df), "linhas")
