from __future__ import annotations
from app.settings import SETTINGS

def calc_floor(cost: float) -> float:
    # Regras pedidas:
    # Até 10€ → 60%; 10,01–20€ → 50%; 20,01–40€ → 40%; 40,01–80€ → 30%; >80€ → 25%
    if cost <= 10:
        margin = 0.60
    elif cost <= 20:
        margin = 0.50
    elif cost <= 40:
        margin = 0.40
    elif cost <= 80:
        margin = 0.30
    else:
        margin = 0.25

    base = cost * (1 + margin)
    base += SETTINGS.shipping_cost + SETTINGS.surcharge
    base *= (1 + SETTINGS.amazon_fee_rate)
    base *= (1 + SETTINGS.vat_rate)  # IVA 23%
    return round(base, 2)

def choose_price(cost: float, competitor_price: float | None) -> float:
    floor = calc_floor(cost)
    if competitor_price is None:
        return floor
    candidate = round(max(floor, competitor_price - SETTINGS.undercut_eur), 2)
    return candidate
