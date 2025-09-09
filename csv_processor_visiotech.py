# csv_processor_visiotech.py
# -*- coding: utf-8 -*-
import os, json, re, pandas as pd
from typing import Dict, Tuple, List
from pricing_engine import calc_final_price

CFG_FILE = "config.json"

def load_cfg() -> Dict:
    with open(CFG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def _to_float(x):
    try: return float(str(x).strip().replace(",", "."))
    except: return 0.0

def _to_int(x):
    try: return int(float(str(x).strip().replace(",", ".")))
    except: return 0

# ---------- Leitura robusta ----------
def _safe_read_csv(path_csv: str) -> Tuple[pd.DataFrame, str]:
    attempts = []
    attempts.append(dict(sep=None, engine="python", encoding="utf-8-sig", on_bad_lines="error"))
    attempts.append(dict(sep=None, engine="python", encoding="latin1",     on_bad_lines="error"))
    for sep in [";", ",", "\t", "|"]:
        attempts.append(dict(sep=sep, engine="python", encoding="utf-8-sig", on_bad_lines="error"))
        attempts.append(dict(sep=sep, engine="python", encoding="latin1",     on_bad_lines="error"))
    attempts.append(dict(sep=None, engine="python", encoding="utf-8-sig", on_bad_lines="skip"))
    attempts.append(dict(sep=None, engine="python", encoding="latin1",     on_bad_lines="skip"))

    errors = []
    for opts in attempts:
        try:
            df = pd.read_csv(path_csv, dtype=str, **opts).fillna("")
            return df, f"OK {opts}"
        except Exception as e:
            errors.append(f"{opts} -> {e}")
    raise RuntimeError("Falha a ler CSV do fornecedor:\n" + "\n".join(errors))

# ---------- Mapeamento de colunas ----------
MAP_CANDIDATES = {
    # no teu CSV não há sku -> vamos gerar fallback mais abaixo
    "sku":      ["sku","cod","codigo","code","reference","ref","ref_proveedor","supplier_ref"],
    "brand":    ["brand","marca","manufacturer","fabricante"],
    "ean":      ["ean","gtin","barcode","codigo_barras","cod_barras","upc"],
    "title":    ["title","titulo","name","nombre","descricao","descrição","description","descripcion"],
    "category": ["category","categoria","familia","familía","family","familia_producto","category_parent"],
    # custo no teu CSV: "precio_neto_compra"
    "cost":     ["precio_neto_compra","precio_compra","precio_neto","net_cost","purchase_price",
                 "cost","custo","price_cost","preco_custo","precio_coste","precio","price","net_price","pvd","pneto","pcoste"],
    "stock":    ["stock","qty","quantity","cantidad","qtd","existencias","disponible","availability"]
}

def _choose_first(df: pd.DataFrame, choices: List[str]) -> str:
    for c in choices:
        if c in df.columns and not df[c].eq("").all():
            return c
    return ""

def _slug(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "-", s.strip(), flags=re.UNICODE)
    s = re.sub(r"-+", "-", s).strip("-")
    return s.upper()[:40] if s else ""

def _generate_sku(row: dict) -> str:
    ean = str(row.get("ean","")).strip()
    if ean:
        return ean  # melhor fallback: usar o EAN como SKU
    brand = str(row.get("brand","")).strip()
    title = str(row.get("title","")).strip()
    base = "-".join([x for x in (_slug(brand), _slug(title)) if x])
    return base or "SKU-" + _slug(title) or "SKU-" + _slug(brand) or "SKU-AUTO"

def _map_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str,str]]:
    chosen = {}
    for canon, candidates in MAP_CANDIDATES.items():
        col = _choose_first(df, candidates)
        chosen[canon] = col

    norm = pd.DataFrame()
    for canon in MAP_CANDIDATES.keys():
        col = chosen.get(canon) or ""
        norm[canon] = df[col] if col else ""

    # Fallbacks críticos
    # SKU: se vazio, gerar a partir de EAN ou (brand+title)
    if "sku" not in norm.columns:
        norm["sku"] = ""
    if norm["sku"].eq("").all():
        norm["sku"] = norm.apply(lambda r: _generate_sku(r), axis=1)

    # Custo: se não encontrou nenhuma coluna, cria 0.0; caso tenha valores vazios, força 0.0
    if "cost" not in norm.columns:
        norm["cost"] = 0.0
    # Stock: idem
    if "stock" not in norm.columns:
        norm["stock"] = 0

    # anexar restantes colunas originais (só por segurança/consulta)
    for c in df.columns:
        if c not in norm.columns:
            norm[c] = df[c]

    return norm.fillna(""), chosen

# ---------- Processo principal ----------
def process_csv(path_csv: str, cfg: Dict) -> pd.DataFrame:
    os.makedirs("data", exist_ok=True)

    # 1) ler
    df_raw, how = _safe_read_csv(path_csv)
    # 2) mapear colunas
    df, chosen = _map_columns(df_raw)

    # 3) normalizar tipos
    df["cost"]  = df["cost"].map(_to_float) if "cost" in df.columns else 0.0
    df["stock"] = df["stock"].map(_to_int)  if "stock" in df.columns else 0

    # 4) filtro por marcas permitidas (case-insensitive)
    allowed_cfg = cfg.get("allowed_brands") or []
    if allowed_cfg:
        allowed = {str(b).strip().casefold() for b in allowed_cfg}
        df = df[df["brand"].astype(str).str.strip().str.casefold().isin(allowed)].copy()

    # 5) calcular preços (floor e preview)
    previews, floors = [], []
    for _, r in df.iterrows():
        out = calc_final_price(cost=float(r.get("cost",0.0)),
                               competitor_price=None,
                               cfg=cfg)
        previews.append(out["final_price"])
        floors.append(out["floor_price"])
    df["preview_price"] = previews
    df["floor_price"]   = floors
    df["selling_price"] = df["preview_price"]
    df["status"]        = "ativo"

    # 6) logs de diagnóstico úteis
    try:
        with open("data/_last_csv_read_info.txt","w",encoding="utf-8") as f:
            f.write(how + "\n\n")
            f.write("Mapeamento de colunas detectado:\n")
            for k,v in chosen.items():
                f.write(f"- {k}: {v or '(não encontrado)'}\n")
            f.write("\nPrimeiras colunas do ficheiro original:\n")
            f.write(", ".join(df_raw.columns) + "\n\n")
            f.write("Amostra processada (5 linhas):\n")
            f.write(df[["sku","brand","title","ean","cost","stock","preview_price","floor_price"]].head(5).to_csv(index=False))
            f.write("\n\nEstatísticas:\n")
            f.write(f"Linhas totais (após filtro marcas): {len(df)}\n")
            f.write(f"Cost zero %: {round((df['cost']<=0).mean()*100,2) if len(df) else 0}%\n")
            f.write(f"Stock zero %: {round((df['stock']<=0).mean()*100,2) if len(df) else 0}%\n")
    except Exception:
        pass

    # 7) persistir
    out_path = "data/produtos_processados.csv"
    df.to_csv(out_path, index=False)
    return df
