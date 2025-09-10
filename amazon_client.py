import os
import json
import hashlib
import hmac
import time
from datetime import datetime
from urllib.parse import urljoin
from typing import Dict, Optional

import requests
from dotenv import load_dotenv

# Carregar variáveis a partir de um ficheiro .env, se presente
load_dotenv()

# ---------------------------------------------------------------------------
# Configuração das credenciais
# ---------------------------------------------------------------------------

# Identificador do vendedor (SellerId) e marketplace; obrigatórios
SELLER_ID: Optional[str] = os.getenv("SELLER_ID")
MARKETPLACE_ID: Optional[str] = os.getenv("MARKETPLACE_ID")

# Endpoint base da SP‑API (por omissão utiliza a região europeia)
SPAPI_ENDPOINT: str = os.getenv("SPAPI_ENDPOINT", "https://sellingpartnerapi-eu.amazon.com")

# Credenciais LWA para obter access token
LWA_CLIENT_ID: Optional[str] = os.getenv("LWA_CLIENT_ID")
LWA_CLIENT_SECRET: Optional[str] = os.getenv("LWA_CLIENT_SECRET")
LWA_REFRESH_TOKEN: Optional[str] = os.getenv("LWA_REFRESH_TOKEN")

# Chaves AWS para assinatura SigV4
AWS_ACCESS_KEY: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION: str = os.getenv("AWS_REGION", "eu-west-1")

# Validar que todas as variáveis essenciais estão presentes. Caso contrário,
# lança um erro na importação.
if not all([
    SELLER_ID,
    MARKETPLACE_ID,
    LWA_CLIENT_ID,
    LWA_CLIENT_SECRET,
    LWA_REFRESH_TOKEN,
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
]):
    raise RuntimeError(
        "Faltam variáveis obrigatórias nos secrets/env. "
        "Verifique SELLER_ID, MARKETPLACE_ID, LWA_CLIENT_ID, LWA_CLIENT_SECRET, "
        "LWA_REFRESH_TOKEN, AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY."
    )

# ---------------------------------------------------------------------------
# Funções auxiliares para a autenticação e assinatura SigV4
# ---------------------------------------------------------------------------

def get_lwa_access_token() -> str:
    """Obtém um token de acesso LWA usando o refresh token configurado."""
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": LWA_REFRESH_TOKEN,
        "client_id": LWA_CLIENT_ID,
        "client_secret": LWA_CLIENT_SECRET,
    }
    resp = requests.post(url, data=data, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def _sign(key: bytes, msg: str) -> bytes:
    """Assina uma mensagem com a chave fornecida usando HMAC-SHA256."""
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _get_signature_key(key: str, date_stamp: str, region_name: str, service_name: str) -> bytes:
    """Gera a chave de assinatura derivada para AWS SigV4."""
    k_date = _sign(("AWS4" + key).encode("utf-8"), date_stamp)
    k_region = _sign(k_date, region_name)
    k_service = _sign(k_region, service_name)
    k_signing = _sign(k_service, "aws4_request")
    return k_signing


def _amz_datetime() -> tuple[str, str]:
    """Devolve a data/hora actual em formatos ISO e data (YYYYMMDD)."""
    now = datetime.utcnow()
    return now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")


def _canonical_request(method: str, path: str, querystring: str, headers: Dict[str, str], payload_hash: str) -> str:
    """Constrói a string de request canónica usada na assinatura SigV4."""
    signed_headers = ";".join([h.lower() for h in headers.keys()])
    canonical_headers = "".join([f"{h.lower()}:{headers[h].strip()}\n" for h in headers.keys()])
    return "\n".join([
        method,
        path,
        querystring or "",
        canonical_headers,
        signed_headers,
        payload_hash,
    ])


def _string_to_sign(amz_date: str, date_stamp: str, region: str, service: str, canonical_request: str) -> str:
    """Cria a string a assinar para SigV4."""
    cr_hash = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    return "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        f"{date_stamp}/{region}/{service}/aws4_request",
        cr_hash,
    ])


def _authorization_header(access_key: str, signed_headers: str, signature: str, date_stamp: str, region: str, service: str) -> str:
    """Forma o valor do cabeçalho Authorization para SigV4."""
    credential = f"{access_key}/{date_stamp}/{region}/{service}/aws4_request"
    return (
        f"AWS4-HMAC-SHA256 Credential={credential}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )


def _signed_request(
    method: str,
    path: str,
    params: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict] = None,
    access_token: Optional[str] = None,
) -> Dict:
    """
    Envia um pedido HTTP assinado via SigV4 para a SP‑API.

    Args:
        method: Método HTTP (GET, POST, PUT, PATCH).
        path: Caminho do endpoint (por exemplo, "/listings/2021-08-01/items/{sellerId}/{sku}").
        params: Parâmetros de querystring a incluir na URL.
        json_body: Corpo JSON para métodos POST/PUT/PATCH.
        access_token: Token LWA obtido previamente.

    Returns:
        A resposta JSON do endpoint. Em caso de erro HTTP (>=400), lança RuntimeError.
    """
    service = "execute-api"
    host = SPAPI_ENDPOINT.replace("https://", "").strip("/")
    url = urljoin(SPAPI_ENDPOINT + "/", path.lstrip("/"))

    # Preparar o corpo e respectivo hash
    body = (
        json.dumps(json_body or {}, separators=(",", ":"), ensure_ascii=False)
        if method in ("POST", "PUT", "PATCH")
        else ""
    )
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    # Data/hora AWS
    amz_date, date_stamp = _amz_datetime()
    headers: Dict[str, str] = {
        "host": host,
        "x-amz-date": amz_date,
        "x-amz-access-token": access_token or "",
        "content-type": "application/json; charset=utf-8",
        "accept": "application/json",
    }

    # Construir canonical request sem querystring (params vão no URL)
    canonical = _canonical_request(
        method,
        url.replace(SPAPI_ENDPOINT, "").split("?", 1)[0],
        "",
        headers,
        payload_hash,
    )
    sts = _string_to_sign(amz_date, date_stamp, AWS_REGION, service, canonical)
    signing_key = _get_signature_key(AWS_SECRET_KEY, date_stamp, AWS_REGION, service)
    signature = hmac.new(signing_key, sts.encode("utf-8"), hashlib.sha256).hexdigest()

    signed_headers = ";".join([h.lower() for h in headers.keys()])
    headers["Authorization"] = _authorization_header(
        AWS_ACCESS_KEY,
        signed_headers,
        signature,
        date_stamp,
        AWS_REGION,
        service,
    )

    # Executar o request
    resp = requests.request(
        method,
        url,
        params=params,
        data=body if body else None,
        headers=headers,
        timeout=60,
    )
    if resp.status_code >= 400:
        raise RuntimeError(
            f"SP-API {method} {path} falhou: {resp.status_code} {resp.text[:500]}"
        )
    return resp.json() if resp.text else {}


# ---------------------------------------------------------------------------
# Funções principais da Listings Items API
# ---------------------------------------------------------------------------

def patch_listings_item(sku: str, quantity: int, price_with_tax_eur: float) -> Dict:
    """
    Actualiza a disponibilidade e o preço de um SKU existente na Amazon.

    Esta função constrói um documento JSON de patches que altera:
      • ``/attributes/fulfillmentAvailability`` – para indicar a quantidade em
        stock no canal ``DEFAULT``;
      • ``/attributes/purchasableOffer`` – para actualizar o preço final com
        imposto incluído.

    Args:
        sku: Identificador do produto no seller central.
        quantity: Quantidade disponível a enviar.
        price_with_tax_eur: Preço final (PVP) com IVA incluído, em euros.

    Returns:
        Resposta da SP‑API (JSON) em caso de sucesso.
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
                "value": [
                    {
                        "fulfillmentChannelCode": "DEFAULT",
                        "quantity": int(quantity),
                    }
                ],
            },
            {
                "op": "replace",
                "path": "/attributes/purchasableOffer",
                "value": [
                    {
                        "currency": "EUR",
                        "ourPrice": [
                            {
                                "schedule": [
                                    {
                                        "valueWithTax": {
                                            "amount": f"{price_with_tax_eur:.2f}",
                                            "currency": "EUR",
                                        }
                                    }
                                ]
                            }
                        ],
                    }
                ],
            },
        ],
    }
    return _signed_request(
        "PATCH", path, params=params, json_body=body, access_token=access_token
    )


def put_listings_item(
    sku: str,
    product_type: str,
    attributes: Dict,
    requirements: str = "LISTING",
) -> Dict:
    """
    Cria ou substitui uma listagem completa para um SKU ainda não associado a um ASIN.

    Args:
        sku: Identificador interno do produto a ser listado.
        product_type: Tipo de produto (por exemplo, "camera", "lock"). Deve
            corresponder ao resultado da API de definições de tipo de produto.
        attributes: Dicionário de atributos conforme o schema do product type. É
            responsabilidade do chamador garantir que os campos obrigatórios
            estão presentes.
        requirements: String que indica o nível de requisitos (por omissão,
            "LISTING"). Pode ser "LISTING" ou "LISTING_OFFER_ONLY".

    Returns:
        Resposta da SP‑API (JSON) em caso de sucesso.
    """
    access_token = get_lwa_access_token()
    path = f"/listings/2021-08-01/items/{SELLER_ID}/{sku}"
    params = {"marketplaceIds": MARKETPLACE_ID}
    body = {
        "productType": product_type,
        "requirements": requirements,
        "attributes": attributes,
    }
    return _signed_request(
        "PUT", path, params=params, json_body=body, access_token=access_token
    )


# ---------------------------------------------------------------------------
# Classe de compatibilidade
# ---------------------------------------------------------------------------

class AmazonClient:
    """
    Wrapper orientado a objectos sobre as funções básicas da Listings Items API.

    Esta classe existe para manter compatibilidade com o código legado que
    instanciava ``AmazonClient``. A maioria das operações é delegada para
    funções modulares definidas acima. A propriedade ``simulate`` permite
    efectuar execuções em modo simulado, em que nenhuma chamada real é
    transmitida à Amazon.
    """

    def __init__(self, simulate: Optional[bool] = None):
        # Se simulate não for especificado, procura a variável de ambiente
        if simulate is None:
            simulate_env = os.getenv("SPAPI_SIMULATE", "true").lower()
            simulate = simulate_env in ("1", "true", "yes", "on")
        self.simulate = bool(simulate)

    def _get_token(self) -> str:
        """
        Devolve um access token LWA. Em modo simulado, devolve a string
        ``SIMULATED``.
        """
        if self.simulate:
            return "SIMULATED"
        return get_lwa_access_token()

    # Métodos de alto nível para o Flask ou outros consumidores
    def patch_price_stock(self, sku: str, quantity: int, price_with_tax_eur: float) -> Dict:
        """
        Actualiza a disponibilidade e o preço de um SKU. Em modo simulado,
        devolve um dicionário indicativo sem efectuar chamadas à Amazon.
        """
        if self.simulate:
            return {
                "simulate": True,
                "operation": "patch",
                "sku": sku,
                "quantity": quantity,
                "price": price_with_tax_eur,
            }
        return patch_listings_item(sku, quantity, price_with_tax_eur)

    def create_or_update_listing(
        self, sku: str, product_type: str, attributes: Dict, requirements: str = "LISTING"
    ) -> Dict:
        """
        Cria ou actualiza uma listagem completa para um SKU. Em modo simulado,
        devolve um dicionário indicativo.
        """
        if self.simulate:
            return {
                "simulate": True,
                "operation": "put",
                "sku": sku,
                "product_type": product_type,
            }
        return put_listings_item(sku, product_type, attributes, requirements)
