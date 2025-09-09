# amazon_client.py
# -*- coding: utf-8 -*-
import os, time, json, logging, requests, gzip, io
from dotenv import load_dotenv
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

load_dotenv()
log = logging.getLogger(__name__)

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
SP = os.getenv("SPAPI_ENDPOINT","https://sellingpartnerapi-eu.amazon.com")
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID","A1RKKUPIHCS9HS")

class AmazonClient:
    def __init__(self, simulate=True):
        self.simulate = simulate
        self.client_id = os.getenv("LWA_CLIENT_ID")
        self.client_secret = os.getenv("LWA_CLIENT_SECRET")
        self.refresh_token = os.getenv("LWA_REFRESH_TOKEN")
        self.aws_access = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region = os.getenv("AWS_REGION","eu-west-1")
        self.token = None
        self.token_exp = 0

    # --------------- auth ---------------
    def _get_token(self):
        if self.simulate:
            return "SIMULATED"
        if self.token and time.time() < self.token_exp:
            return self.token
        data = {
            "grant_type":"refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        r = requests.post(LWA_TOKEN_URL, data=data, timeout=30)
        r.raise_for_status()
        j = r.json()
        self.token = j["access_token"]
        self.token_exp = time.time() + j.get("expires_in",3600) - 60
        return self.token

    def _signed(self, method, path, params=None, body=b"", headers=None):
        if self.simulate:
            class R:
                status_code = 200
                text = "SIMULATED"
                def json(self_inner): return {"simulated": True}
            return R()
        url = f"{SP}{path}"
        headers = headers or {}
        headers["x-amz-access-token"] = self._get_token()
        req = AWSRequest(method=method, url=url, params=params or {}, data=body, headers=headers)
        creds = Credentials(self.aws_access, self.aws_secret)
        SigV4Auth(creds, "execute-api", self.region).add_auth(req)
        prepped = req.prepare()
        return requests.request(method, prepped.url, headers=dict(prepped.headers), params=params, data=body, timeout=60)

    # --------------- feeds (submit + status + reports) ---------------
    def _create_feed_document(self, content_type="text/tab-separated-values; charset=UTF-8"):
        path = "/feeds/2021-06-30/documents"
        headers = {"content-type":"application/json"}
        body = json.dumps({"contentType": content_type})
        r = self._signed("POST", path, headers=headers, body=body.encode("utf-8"))
        r.raise_for_status()
        return r.json()

    def _upload_document(self, doc_info, content: bytes):
        upload_url = doc_info["url"]
        h = {"Content-Type": doc_info.get("contentType","text/tab-separated-values; charset=UTF-8")}
        r = requests.put(upload_url, data=content, headers=h, timeout=60)
        r.raise_for_status()

    def submit_tsv(self, feed_type: str, tsv_str: str) -> str:
        if self.simulate:
            log.info("SIMULATED submit_tsv %s (%d bytes)", feed_type, len(tsv_str))
            return f"SIM-{int(time.time())}"
        doc = self._create_feed_document()
        self._upload_document(doc, tsv_str.encode("utf-8"))
        path = "/feeds/2021-06-30/feeds"
        headers = {"content-type":"application/json"}
        body = {
            "feedType": feed_type,
            "marketplaceIds": [MARKETPLACE_ID],
            "inputFeedDocumentId": doc["feedDocumentId"]
        }
        r = self._signed("POST", path, headers=headers, body=json.dumps(body).encode("utf-8"))
        r.raise_for_status()
        return r.json()["feedId"]

    def get_feed(self, feed_id: str):
        if self.simulate:
            return {"feedId": feed_id, "processingStatus": "DONE", "resultFeedDocumentId": None, "simulated": True}
        path = f"/feeds/2021-06-30/feeds/{feed_id}"
        r = self._signed("GET", path)
        r.raise_for_status()
        return r.json()

    def wait_feed(self, feed_id: str, timeout_sec=420, poll_every=10):
        if self.simulate:
            return {"feedId": feed_id, "processingStatus": "DONE", "resultFeedDocumentId": "SIM-DOC", "simulated": True}
        t0 = time.time()
        while True:
            j = self.get_feed(feed_id)
            st = j.get("processingStatus")
            if st in ("DONE","CANCELLED","FATAL","ERROR"):
                return j
            if time.time()-t0 > timeout_sec:
                return j
            time.sleep(poll_every)

    def get_feed_document(self, feed_document_id: str):
        if self.simulate:
            return {"url":"", "compressionAlgorithm":"GZIP", "simulated":True}
        path = f"/feeds/2021-06-30/documents/{feed_document_id}"
        r = self._signed("GET", path)
        r.raise_for_status()
        return r.json()

    def download_report_text(self, feed_document_id: str) -> str:
        if self.simulate:
            return "SIMULATED REPORT"
        doc = self.get_feed_document(feed_document_id)
        url = doc.get("url")
        alg = (doc.get("compressionAlgorithm") or "").upper()
        rr = requests.get(url, timeout=60)
        rr.raise_for_status()
        data = rr.content
        if alg == "GZIP":
            try:
                data = gzip.decompress(data)
            except Exception:
                data = gzip.GzipFile(fileobj=io.BytesIO(rr.content)).read()
        try:
            return data.decode("utf-8", errors="ignore")
        except Exception:
            return data.decode("latin-1", errors="ignore")

    def save_processing_report(self, feed_id: str, feed_type: str) -> str | None:
        """Guarda o relatório txt e devolve o caminho do ficheiro, se existir."""
        try:
            j = self.get_feed(feed_id)
            doc_id = j.get("resultFeedDocumentId")
            if not doc_id:
                return None
            txt = self.download_report_text(doc_id)
            os.makedirs("data/reports", exist_ok=True)
            name = f"data/reports/feed_{feed_type.replace('POST_','').replace('_DATA','').lower()}_{feed_id}.txt"
            with open(name, "w", encoding="utf-8") as f:
                f.write(txt)
            return name
        except Exception as e:
            log.warning("Falha a descarregar relatório do feed %s: %s", feed_id, e)
            return None

    # --------------- helpers TSV (headers compatíveis) ---------------
    def feed_inventory_tsv(self, df):
        """POST_INVENTORY_AVAILABILITY_DATA — usar 'fulfillment-channel' ajuda a compatibilidade MFN."""
        lines = ["sku\tquantity\tfulfillment-channel"]
        for _, r in df.iterrows():
            qty = int(float(r.get("stock", 0) or 0))
            lines.append(f"{r['sku']}\t{qty}\tDEFAULT")
        return "\n".join(lines)

    def feed_pricing_tsv(self, df):
        """POST_PRODUCT_PRICING_DATA — incluir 'currency' melhora compatibilidade."""
        lines = ["sku\tprice\tcurrency"]
        for _, r in df.iterrows():
            price = float(r.get("selling_price", r.get("price", 0)) or 0)
            lines.append(f"{r['sku']}\t{price:.2f}\tEUR")
        return "\n".join(lines)

    # --------------- pricing concorrência (opcional) ---------------
    def get_listing_offers(self, asin: str):
        if self.simulate:
            return []
        path = f"/products/pricing/v0/listings/{asin}/offers"
        params = {"MarketplaceId": MARKETPLACE_ID}
        r = self._signed("GET", path, params=params)
        if r.status_code != 200:
            return []
        try:
            j = r.json()
        except Exception:
            return []
        offers = []
        for o in ((j.get("payload") or {}).get("Offers") or []):
            lp = 0.0
            try:
                lp = float(((o.get("ListingPrice") or {}).get("Amount") or 0.0)) + float(((o.get("Shipping") or {}).get("Amount") or 0.0))
            except Exception:
                lp = 0.0
            offers.append({"LandedPrice": lp})
        return offers
