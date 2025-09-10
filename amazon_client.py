# amazon_client.py
# Cliente SP-API minimalista, compatível com os teus secrets e endpoints.
# Opera com Listings Items API: PATCH (stock/preço) e PUT (criação).
# Sem dependências externas além de requests.

import os, time, json, base64, hashlib, hmac
from datetime import datetime
from urllib.parse import urljoin
import requests
from dotenv import load_dotenv

load_dotenv()

# ---------- ENV / CONFIG ----------
SELLER_ID        = os.getenv("SELLER_ID")
MARKETPLACE_ID   = os.getenv("MARKETPLACE_ID")
SPAPI_ENDPOINT   = os.getenv("SPAPI_ENDPOINT", "https://sellingpartnerapi-eu.amazon.com")
LWA_CLIENT_ID    = os.getenv("LWA_CLIENT_ID")
LWA_CLIENT_SECRET= os.getenv("LWA_CLIENT_SECRET")
LWA_REFRESH_TOKEN= os.getenv("LWA_REFRESH_TOKEN")
AWS_ACCESS_KEY   = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY   = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION       = os.getenv("AWS_REGION", "eu-west-1")

if not all([SELLER_ID, MARKETPLACE_ID, LWA_CLIENT_ID, LWA_CLIENT_SECRET, LWA_REFRESH_TOKEN, AWS_ACCESS_KEY, AWS_SECRET_KEY]):
    raise RuntimeError("Faltam variáveis obrigatórias nos secrets/env.")

# ---------- LWA TOKEN ----------
def get_lwa_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": LWA_REFRESH_TOKEN,
        "client_id": LWA_CLIENT_ID,
        "client_secret": LWA_CLIENT_SECRET,
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

# ---------- AWS SigV4 mínimo ----------
def _sign(key, msg): return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()
def _get_signature_key(key, date_stamp, regionName, serviceName):
    kDate    = _sign(("AWS4" + key).encode("utf-8"), date_stamp)
    kRegion  = _sign(kDate, regionName)
    kService = _sign(kRegion, serviceName)
    kSigning = _sign(kService, "aws4_request")
    return kSigning

def _amz_datetime():
    now = datetime.utcnow()
    return now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")

def _canonical_request(method, path, querystring, headers, payload_hash):
    signed_headers = ";".join([h.lower() for h in headers.keys()])
    canonical_headers = "".join([f"{h.lower()}:{headers[h].strip()}\n" for h in headers.keys()])
    return "\n".join([
        method,
        path,
        querystring or "",
        canonical_headers,
        signed_headers,
        payload_hash
    ])

def _string_to_sign(amz_date, date_stamp, region, service, canonical_request):
    cr_hash = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    return "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        f"{date_stamp}/{region}/{service}/aws4_request",
        cr_hash
    ])

def _authorization_header(access_key, signed_headers, signature, date_stamp, region, service):
    credential = f"{access_key}/{date_stamp}/{region}/{service}/aws4_request"
    return f"AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders={signed_headers}, Signature={signature}"

def _signed_request(method, path, params=None, json_body=None, access_token=None):
    service = "execute-api"
    host = SPAPI_ENDPOINT.replace("https://", "").strip("/")
    url  = urljoin(SPAPI_ENDPOINT + "/", path.lstrip("/"))

    body = json.dumps(json_body or {}, separators=(",", ":"), ensure_ascii=False) if method in ("POST","PUT","PATCH") else ""
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    amz_date, date_stamp = _amz_datetime()
    headers = {
        "host": host,
        "x-amz-date": amz_date,
        "x-amz-access-token": access_token or "",
        "content-type": "application/json; charset=utf-8",
        "accept": "application/json",
    }
    # querystring vazio (usamos params no requests só para URL)
    canonical = _canonical_request(method, url.replace(SPAPI_ENDPOINT, "").split("?",1)[0], "", headers, payload_hash)
    sts = _string_to_sign(amz_date, date_stamp, AWS_REGION, service, canonical)
    signing_key = _get_signature_key(AWS_SECRET_KEY, date_stamp, AWS_REGION, service)
    signature = hmac.new(signing_key, sts.encode("utf-8"), hashlib.sha256).hexdigest()

    signed_headers = ";".join([h.lower() for h in headers.keys()])
    headers["Authorization"] = _authorization_header(AWS_ACCESS_KEY, signed_headers, signature, date_stamp, AWS_REGION, service)

    r = requests.request(method, url, params=params, data=body if body else None, headers=headers, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"SP-API {method} {path} falhou: {r.status_code} {r.text[:500]}")
    return r.json() if r.text else {}

# ---------- Listings Items API ----------
def patch_listings_item(sku: str, quantity: int, price_with_tax_eur: float):
    """
    Atualiza stock e preço (com IVA) via PATCH.
    """
    access_token = get_lwa_access_token()
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{sku}"
    params = {"marketplaceIds": MARKETPLACE_ID}
    body = {
        "productType": "PRODUCT",
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/fulfillmentAvailability",
                "value": [{"fulfillmentChannelCode":"DEFAULT","quantity": int(quantity)}]
            },
            {
                "op": "replace",
                "path": "/attributes/purchasableOffer",
                "value": [{
                    "currency": "EUR",
                    "ourPrice": [{
                        "schedule": [{
                            "valueWithTax": { "amount": f"{price_with_tax_eur:.2f}", "currency":"EUR" }
                        }]
                    }]
                }]
            }
        ]
    }
    return _signed_request("PATCH", path, params=params, json_body=body, access_token=access_token)

def put_listings_item(sku: str, product_type: str, attributes: dict, requirements: str = "LISTING"):
    """
    Cria/atualiza listagem completa para um SKU (quando ainda não tens SKU associado ao ASIN).
    Necessita product_type válido e attributes segundo Product Type Definitions.
    """
    access_token = get_lwa_access_token()
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{sku}"
    params = {"marketplaceIds": MARKETPLACE_ID}
    body = {
        "productType": product_type,
        "requirements": requirements,
        "attributes": attributes
    }
    return _signed_request("PUT", path, params=params, json_body=body, access_token=access_token)

# ---------- Catalog Items (resolver ASIN por GTIN/EAN) ----------
def catalog_search_by_gtin(gtin: str):
    access_token = get_lwa_access_token()
    path = "/catalog/2022-04-01/items"
    params = {
        "identifiers": gtin,
        "identifiersType": "GTIN",
        "marketplaceIds": MARKETPLACE_ID,
        "includedData": "attributes,identifiers,summaries,relationships"
    }
    return _signed_request("GET", path, params=params, json_body=None, access_token=access_token)
