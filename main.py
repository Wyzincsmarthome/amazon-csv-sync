from csv_processor_visiotech import process_csv, load_cfg
from product_identify import classify_products

cfg = load_cfg()
process_csv("fornecedor.csv", cfg)
classify_products()
