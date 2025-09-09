# amazon_reports.py
# -*- coding: utf-8 -*-
import os, time, json, logging, requests, gzip, io, csv
from typing import Tuple, List
from dotenv import load_dotenv
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

load_dotenv()
log = logging.getLogger(__name__)

SP = os.getenv("SPAPI_ENDPOINT","https://sellingpartnerapi-eu.amazon.com")
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID","A1RKKUPIHCS9HS")

class SpReportsSession:
    def __init__(self, simulate: bool | None = None):
        if simulate is None:
            simulate = os.getenv("SPAPI_SIMULATE","true").lower() in ("1","true","yes","on")
        self.simulate = simulate
        self.client_id = os.getenv("LWA_CLIENT_ID")
        self.client_secret = os.getenv("LWA_CLIENT_SECRET")
        self.refresh_token = os.getenv("LWA_REFRESH_TOKEN")
        self.aws_access = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region = os.getenv("AWS_REGION","eu-west-1")
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
        r.raise_for_status()
        j = r.json()
        self._token = j["access_token"]
        self._exp = time.time() + j.get("expires_in",3600) - 60
        return self._token

    def _signed(self, method: str, path: str, params=None, body: bytes = b"", headers=None):
        if self.simulate:
            class R:
                status_code = 200
                text = "SIMULATED"
                def json(self_inner): return {"simulated": True}
                content = b""
            return R()
        url = f"{SP}{path}"
        headers = headers or {}
        headers["x-amz-access-token"] = self._get_token()
        req = AWSRequest(method=method, url=url, params=params or {}, data=body, headers=headers)
        creds = Credentials(self.aws_access, self.aws_secret)
        SigV4Auth(creds, "execute-api", self.region).add_auth(req)
        prepped = req.prepare()
        return requests.request(method, prepped.url, headers=dict(prepped.headers), params=params, data=body, timeout=60)

    # -------- Reports API 2021-06-30 ----------
    def create_report(self, report_type: str, marketplaces: list[str]) -> str:
        path = "/reports/2021-06-30/reports"
        headers = {"content-type":"application/json"}
        body = {"reportType": report_type, "marketplaceIds": marketplaces}
        r = self._signed("POST", path, headers=headers, body=json.dumps(body).encode("utf-8"))
        r.raise_for_status()
        j = r.json()
        return j["reportId"]

    def get_report(self, report_id: str) -> dict:
        path = f"/reports/2021-06-30/reports/{report_id}"
        r = self._signed("GET", path)
        r.raise_for_status()
        return r.json()

    def wait_report(self, report_id: str, timeout_sec=900, poll_every=10) -> dict:
        if self.simulate:
            return {"reportId":report_id, "processingStatus":"DONE", "reportDocumentId":"SIM-DOC"}
        t0 = time.time()
        while True:
            j = self.get_report(report_id)
            st = j.get("processingStatus")
            if st in ("DONE","CANCELLED","FATAL","ERROR"):
                return j
            if time.time()-t0 > timeout_sec:
                return j
            time.sleep(poll_every)

    def get_report_document(self, report_document_id: str) -> dict:
        path = f"/reports/2021-06-30/documents/{report_document_id}"
        r = self._signed("GET", path)
        r.raise_for_status()
        return r.json()

    def download_report(self, report_document_id: str) -> bytes:
        doc = self.get_report_document(report_document_id)
        url = doc.get("url")
        alg = (doc.get("compressionAlgorithm") or "").upper()
        rr = requests.get(url, timeout=120)
        rr.raise_for_status()
        data = rr.content
        if alg == "GZIP":
            try:
                data = gzip.decompress(data)
            except Exception:
                data = gzip.GzipFile(fileobj=io.BytesIO(rr.content)).read()
        return data

def _safe_replace(src_tmp: str, dst_final: str, retries: int = 5, wait_sec: float = 0.6):
    """Troca atómica com retries para escapar locks do Windows."""
    for _ in range(retries):
        try:
            os.replace(src_tmp, dst_final)
            return
        except PermissionError:
            time.sleep(wait_sec)
    # última tentativa: grava com sufixo timestamp para não perder dados
    alt = dst_final.replace(".csv", f"_{int(time.time())}.csv").replace(".tsv", f"_{int(time.time())}.tsv")
    os.replace(src_tmp, alt)
    raise PermissionError(f"Não consegui substituir {dst_final}. O ficheiro pode estar aberto. Gravei como {alt}.")

def fetch_my_inventory(simulate: bool | None = None) -> Tuple[str, int]:
    """
    Pede GET_MERCHANT_LISTINGS_ALL_DATA, descarrega e guarda TSV "data/my_inventory_raw.tsv"
    (escrita segura com .tmp). Converte para CSV "data/my_inventory.csv"
    com colunas: asin,seller_sku,price,quantity,condition,status
    Devolve (ficheiro_csv, linhas).
    """
    os.makedirs("data", exist_ok=True)
    sess = SpReportsSession(simulate=simulate)
    if sess.simulate:
        # inventário fictício
        rows = [
            {"asin":"B0CW3J3F71","seller_sku":"AJ-DOORPROTECTPLUS-W","price":"59.99","quantity":"10","condition":"New","status":"Active"},
        ]
        out_csv = "data/my_inventory.csv"
        tmp_csv = out_csv + ".tmp"
        with open(tmp_csv,"w",encoding="utf-8",newline="") as f:
            w = csv.DictWriter(f, fieldnames=["asin","seller_sku","price","quantity","condition","status"])
            w.writeheader()
            for r in rows: w.writerow(r)
        _safe_replace(tmp_csv, out_csv)
        return out_csv, len(rows)

    # 1) Criar + esperar relatório
    report_id = sess.create_report("GET_MERCHANT_LISTINGS_ALL_DATA", [MARKETPLACE_ID])
    rep = sess.wait_report(report_id, timeout_sec=900, poll_every=15)
    if rep.get("processingStatus") != "DONE":
        raise RuntimeError(f"Relatório não ficou DONE: {rep.get('processingStatus')}")

    # 2) Descarregar
    doc_id = rep.get("reportDocumentId")
    if not doc_id:
        raise RuntimeError("Sem reportDocumentId no relatório.")
    raw = sess.download_report(doc_id)

    raw_path = "data/my_inventory_raw.tsv"
    tmp_raw = raw_path + ".tmp"
    with open(tmp_raw,"wb") as f:
        f.write(raw)
    _safe_replace(tmp_raw, raw_path)

    # 3) Parse -> CSV
    text = raw.decode("utf-8", errors="ignore")
    lines = [l for l in text.splitlines()]
    if not lines:
        raise RuntimeError("Relatório vazio.")
    header = [h.strip() for h in lines[0].split("\t")]

    # mapeamento robusto
    idx = {h.lower(): i for i, h in enumerate(header)}
    def find(*names):
        for n in names:
            if n in idx: return idx[n]
        return None

    col_asin  = find("asin1","asin","asin-1")
    col_sku   = find("seller-sku","sku","seller sku")
    col_price = find("price")
    col_qty   = find("quantity","qty")
    col_cond  = find("item-condition","condition-type")
    col_stat  = find("status","status_of_listing","status_of_sale")

    out_rows: List[dict] = []
    for line in lines[1:]:
        if not line.strip(): 
            continue
        parts = line.split("\t")
        def get(i):
            return parts[i].strip() if (i is not None and i < len(parts)) else ""
        out_rows.append({
            "asin": get(col_asin),
            "seller_sku": get(col_sku),
            "price": get(col_price),
            "quantity": get(col_qty),
            "condition": get(col_cond),
            "status": get(col_stat),
        })

    out_csv = "data/my_inventory.csv"
    tmp_csv = out_csv + ".tmp"
    with open(tmp_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["asin","seller_sku","price","quantity","condition","status"])
        w.writeheader()
        for r in out_rows: 
            w.writerow(r)
    _safe_replace(tmp_csv, out_csv)

    return out_csv, len(out_rows)
