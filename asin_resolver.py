# asin_resolver.py
# -*- coding: utf-8 -*-
import os
import re
import time
import json
import datetime
import logging
import requests
from urllib.parse import quote
from difflib import SequenceMatcher

from dotenv import load_dotenv
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

load_dotenv()
log = logging.getLogger(__name__)

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
SP = os.getenv("SPAPI_ENDPOINT", "https://sellingpartnerapi-eu.amazon.com")
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID", "A1RKKUPIHCS9HS")  # ES

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, _norm(a), _norm(b)).ratio()

def _ts() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def _log_diag(payload: dict) -> None:
    try:
        os.makedirs("logs", exist_ok=True)
        with open("logs/catalog_lookup.log", "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass

class SpaSession:
    def __init__(self, simulate: bool | None = None):
        if simulate is None:
            simulate = os.getenv("SPAPI_SIMULATE", "true").lower() in ("1", "true", "yes", "on")
        self.simulate = simulate

        self.client_id = os.getenv("LWA_CLIENT_ID")
        self.client_secret = os.getenv("LWA_CLIENT_SECRET")
        self.refresh_token = os.getenv("LWA_REFRESH_TOKEN")
        self.aws_access = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region = os.getenv("AWS_REGION", "eu-west-1")

        self._token = None
        self._exp = 0

    def _get_token(self) -> str:
        if self.simulate:
            return "SIMULATED"
        if self._token and time.time() < self._exp:
            return self._token
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        r = requests.post(LWA_TOKEN_URL, data=data, timeout=30)
        _log_diag({"ts": _ts(), "op": "lwa_token", "status": r.status_code, "text": r.text[:300]})
        r.raise_for_status()
        j = r.json()
        self._token = j["access_token"]
        self._exp = time.time() + j.get("expires_in", 3600) - 60
        return self._token

    def _signed(self, method: str, path: str, params: dict | None = None, body: bytes = b"", headers: dict | None = None):
        if self.simulate:
            class R:
                status_code = 200
                text = "SIMULATED"
                def json(self_inner): return {"items": []}
            _log_diag({"ts": _ts(), "op": "simulate_call", "path": path, "params": params})
            return R()

        url = f"{SP}{path}"
        headers = headers or {}
        headers["x-amz-access-token"] = self._get_token()
        req = AWSRequest(method=method, url=url, params=params or {}, data=body, headers=headers)
        creds = Credentials(self.aws_access, self.aws_secret)
        SigV4Auth(creds, "execute-api", self.region).add_auth(req)
        prepped = req.prepare()
        resp = requests.request(method, prepped.url, headers=dict(prepped.headers), params=params, data=body, timeout=60)
        _log_diag({
            "ts": _ts(),
            "op": "spapi_call",
            "path": path,
            "status": resp.status_code,
            "params": params,
            "sample": resp.text[:400]
        })
        return resp

    def search_by_ean(self, ean: str):
        path = "/catalog/2022-04-01/items"
        params = {
            "identifiers": ean,
            "identifiersType": "EAN",
            "marketplaceIds": MARKETPLACE_ID,
            "includedData": "identifiers,attributes,summaries",
        }
        return self._signed("GET", path, params=params)

    def search_by_keywords(self, keywords: str, brand: str | None = None, with_brand_filter: bool = True):
        path = "/catalog/2022-04-01/items"
        params = {
            "keywords": keywords,
            "marketplaceIds": MARKETPLACE_ID,
            "includedData": "identifiers,attributes,summaries",
        }
        if with_brand_filter and brand:
            params["brandNames"] = brand
        return self._signed("GET", path, params=params)

    def get_listing(self, seller_id: str, sku: str):
        path = f"/listings/2021-08-01/items/{quote(seller_id)}/{quote(sku)}"
        return self._signed("GET", path)

def _extract_model_tokens(name: str) -> list[str]:
    return re.findall(r"[A-Z0-9]{2,}[-_A-Z0-9]*", (name or "").upper())[:6]

def resolve_asin(
    sku: str,
    name: str,
    brand: str,
    ean: str | None,
    seller_id: str | None = None,
    simulate: bool | None = None
) -> dict:
    sess = SpaSession(simulate=simulate)

    if seller_id and sku:
        r = sess.get_listing(seller_id, sku)
        if r.status_code == 200:
            try:
                j = r.json() or {}
                sums = (j.get("summaries") or []) if isinstance(j, dict) else []
                if sums:
                    asin = sums[0].get("asin")
                    if asin:
                        return {"status": "listed", "asin": asin, "score": 1.0, "candidates": []}
            except Exception:
                pass

    if ean:
        r = sess.search_by_ean(ean)
        if r.status_code == 200:
            try:
                j = r.json() or {}
                items = j.get("items") or []
                if items:
                    best_asin = items[0].get("asin")
                    return {"status": "catalog_match", "asin": best_asin, "score": 1.0, "candidates": []}
            except Exception:
                pass

    items = []
    keywords = (name or sku).strip()
    r = sess.search_by_keywords(keywords, brand=brand, with_brand_filter=True)
    if r.status_code == 200:
        try:
            j = r.json() or {}
            items = j.get("items") or []
        except Exception:
            items = []

    if not items:
        r = sess.search_by_keywords(keywords, brand=None, with_brand_filter=False)
        if r.status_code == 200:
            try:
                j = r.json() or {}
                items = j.get("items") or []
            except Exception:
                items = []

    tokens = _extract_model_tokens(name or sku)

    def _brand_score(b, ref):
        return 0.2 if (b and _norm(b) == _norm(ref)) else 0.0

    candidates = []
    best = None
    for it in items:
        summ = ((it.get("summaries") or [{}])[0]) if it.get("summaries") else {}
        title = summ.get("itemName", "") or ""
        it_brand = summ.get("brand", "") or ""
        sc = _brand_score(it_brand, brand)

        sim = _sim(title, f"{brand} {' '.join(tokens)}")
        if sim >= 0.90: sc += 0.15
        elif sim >= 0.80: sc += 0.10

        sc = round(min(sc, 1.0), 2)
        candidates.append({"asin": it.get("asin"), "score": sc, "title": title, "brand": it_brand})
        if (not best) or sc > best["score"]:
            best = {"asin": it.get("asin"), "score": sc}

    if best and best["score"] >= 0.80:
        return {"status": "catalog_match", "asin": best["asin"], "score": best["score"], "candidates": candidates[:5]}
    if candidates:
        return {"status": "catalog_ambiguous", "asin": None, "score": max(c["score"] for c in candidates), "candidates": candidates[:5]}
    return {"status": "not_found", "asin": None, "score": 0.0, "candidates": []}
