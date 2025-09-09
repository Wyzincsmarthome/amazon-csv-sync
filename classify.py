# classify.py
from product_identify import classify_products
df = classify_products(simulate=True)  # deixa True por agora
print("OK -> data/produtos_classificados.csv")
