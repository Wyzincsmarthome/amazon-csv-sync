# pricing_engine.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict

@dataclass
class PricingInputs:
    cost: float
    competitor_price: Optional[float] = None
    cfg: Optional[Dict] = None

def _cfg(pe: Dict, key: str, default):
    # aceita chaves alternativas do teu config.json
    aliases = {
        "amazon_fee_rate": ["amazon_fee_rate", "referral_rate", "fee_rate"],
        "fee_surcharge_rate": ["fee_surcharge_rate", "referral_surcharge_rate", "fee_surcharge"],
        "shipping": ["shipping", "shipment", "transport"],
        "tiers": ["tiers", "margin_tiers"],
        "undercut_step": ["undercut_step", "undercut"],
        "vat": ["vat", "iva"],
    }
    for k in aliases.get(key, [key]):
        if k in pe: return pe[k]
    return default

def _tier_margin(cost: float, cfg: Dict) -> float:
    pe = (cfg or {}).get("pricing_engine", {})
    tiers = _cfg(pe, "tiers", [])
    for t in tiers:
        if cost <= float(t.get("max") or t.get("max_cost") or t.get("max_price") or 9e9):
            return float(t.get("min_margin") or t.get("min_margin_pct") or 0.05)
    return 0.05

def _rounding(price: float, pe: Dict) -> float:
    pat = pe.get("rounding") or None
    if not pat: return round(price + 1e-9, 2)
    p = round(price + 1e-9, 2)
    if pat == ".99":
        inteiro = int(p)
        return float(f"{inteiro}.99") if p - inteiro >= 1 else float(f"{inteiro-1}.99") if p < inteiro else float(f"{inteiro}.99")
    return p

def calc_floor_price(cost: float, cfg: Dict) -> float:
    pe = (cfg or {}).get("pricing_engine", {})
    vat = float(_cfg(pe, "vat", 0.21))
    fee_rate = float(_cfg(pe, "amazon_fee_rate", 0.13))
    fee_surcharge = float(_cfg(pe, "fee_surcharge_rate", 0.02))
    ship = float(_cfg(pe, "shipping", 4.0))
    min_margin = float(_tier_margin(cost, cfg))

    # alvo = custo + lucro_min + transporte (tudo sem IVA, antes das fees)
    target = cost * (1.0 + min_margin) + ship
    # fees aplicam-se ao PVP sem IVA (com surcharge)
    fee_factor = (1.0 - fee_rate * (1.0 + fee_surcharge))
    if fee_factor <= 0: fee_factor = 0.0001
    price_ex_vat = target / fee_factor
    price_inc_vat = price_ex_vat * (1.0 + vat)
    p = _rounding(price_inc_vat, pe)
    min_abs_floor = float(pe.get("min_abs_floor") or 0.0)  # opcional
    if p < min_abs_floor: p = min_abs_floor
    return round(p + 1e-9, 2)

def calc_final_price(p: PricingInputs | None = None, *,
                     cost: Optional[float] = None,
                     competitor_price: Optional[float] = None,
                     cfg: Optional[Dict] = None) -> Dict[str, float]:
    if p is None:
        p = PricingInputs(cost=float(cost or 0.0),
                          competitor_price=competitor_price,
                          cfg=cfg)
    pe = (p.cfg or {}).get("pricing_engine", {})
    floor = calc_floor_price(float(p.cost or 0.0), p.cfg or {})
    under = float(_cfg(pe, "undercut_step", 0.01))
    max_cap = pe.get("max_price_cap")
    if p.competitor_price and p.competitor_price > 0:
        candidate = round(p.competitor_price - under, 2)
        final_price = max(floor, candidate)
    else:
        final_price = floor
    if max_cap:
        try:
            final_price = min(final_price, float(max_cap))
        except:
            pass
    return {"floor_price": float(floor), "final_price": float(round(final_price + 1e-9, 2))}
