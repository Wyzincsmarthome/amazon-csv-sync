#!/usr/bin/env bash
set -euo pipefail
python -m pip install --upgrade pip
pip install -r requirements.txt

# Variáveis padrão (podes editar depois no Codespace)
if [ ! -f .env ]; then
  cat > .env <<EOF
FLASK_SECRET=dev-secret
MAX_CONTENT_LENGTH_MB=300
FLASK_RUN_HOST=0.0.0.0
FLASK_RUN_PORT=5000
DRY_RUN=true
SPAPI_SIMULATE=true
# Preenche antes de envio real:
SELLER_ID=
MARKETPLACE_ID=
LWA_CLIENT_ID=
LWA_CLIENT_SECRET=
LWA_REFRESH_TOKEN=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=eu-west-1
SPAPI_ENDPOINT=https://sellingpartnerapi-eu.amazon.com
EOF
fi

# Arranca a app
python -m flask --app app_flask.py run --host=0.0.0.0 --port=5000 >/tmp/flask.log 2>&1 &
