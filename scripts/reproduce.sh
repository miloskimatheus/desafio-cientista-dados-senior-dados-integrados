#!/usr/bin/env bash
# ==============================================================================
# reproduce.sh — Pipeline completo do projeto, do zero ao mart.
# ------------------------------------------------------------------------------
# Único script que precisa rodar. Em ordem:
#   [1/7] setup_gcp.sh         valida V0/V1/V2 + cria datasets
#   [2/7] load_parquets_bq.sh  bq load --replace dos 4 Parquets
#   [3/7] dbt deps             baixa dbt_utils + dbt_expectations
#   [4/7] dbt debug            sanity check da conexão (sem gastar cota)
#   [5/7] dbt source freshness exercita o gate; warn em `frequencia` é
#                              esperado em dev (ver REPORT.md §3.5 sobre
#                              freshness e §10 sobre ano único 2000)
#   [6/7] dbt build            roda o DAG inteiro (run + test)
#   [7/7] dbt docs generate    produz catálogo + manifest para o lineage
#
# Pré-requisitos (manuais, antes de rodar):
#   export DBT_GCP_PROJECT=<seu-projeto-sandbox>
#   gcloud auth application-default login
#   source .venv/bin/activate
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# Resolve a raiz do projeto a partir do path do script (permite rodar de
# qualquer cwd). O script vive em `scripts/` — sobe um nível.
# ------------------------------------------------------------------------------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# ------------------------------------------------------------------------------
# Pré-requisitos: variável de ambiente + dbt no PATH.
# ------------------------------------------------------------------------------
: "${DBT_GCP_PROJECT:?defina DBT_GCP_PROJECT antes de rodar}"
command -v dbt >/dev/null \
  || { echo "FAIL: dbt ausente no PATH. Ative o venv (source .venv/bin/activate)." >&2; exit 1; }

# ------------------------------------------------------------------------------
# Etapa 1 — infra GCP (idempotente).
# ------------------------------------------------------------------------------
echo "[1/7] setup_gcp.sh"
"$SCRIPT_DIR/setup_gcp.sh"

# ------------------------------------------------------------------------------
# Etapa 2 — carrega os 4 Parquets crus em BigQuery.
# ------------------------------------------------------------------------------
echo "[2/7] load_parquets_bq.sh"
"$SCRIPT_DIR/load_parquets_bq.sh"

# ------------------------------------------------------------------------------
# Etapa 3 — pacotes dbt (dbt_utils, dbt_expectations) em `dbt_packages/`.
# ------------------------------------------------------------------------------
echo "[3/7] dbt deps"
dbt deps

# ------------------------------------------------------------------------------
# Etapa 4 — sanity check. Se profiles.yml ou ADC estiverem quebrados, falha
# AQUI sem consumir cota — o gasto vem só no `dbt build` da etapa 6.
# ------------------------------------------------------------------------------
echo "[4/7] dbt debug --target dev_bq"
dbt debug --target dev_bq

# ------------------------------------------------------------------------------
# Etapa 5 - freshness das 4 sources. `frequencia` usa `data_inicio` como
# loaded_at_field e o sample tem `data=2000` (ano único do sample, ver
# REPORT.md §10); com warn_after=10.950d e error_after=18.250d, hoje a fonte
# ainda PASSA — o WARN só vai aparecer quando a distância para 2000-01-01
# ultrapassar o limiar. `set +e/-e` isola o exit code para que, no dia em
# que o WARN entrar (ou em prod com partições paradas), o pipeline siga
# adiante; em prod, freshness vira gate hard. Saída esperada documentada
# em REPORT.md §4.
# ------------------------------------------------------------------------------
echo "[5/7] dbt source freshness --target dev_bq"
set +e
dbt source freshness --target dev_bq
freshness_exit=$?
set -e
if [[ $freshness_exit -ne 0 ]]; then
  echo "  -> freshness exit=$freshness_exit (warn em frequencia tolerado em dev; ver REPORT.md §3.5)"
fi

# ------------------------------------------------------------------------------
# Etapa 6 — `dbt build` = run + test em ordem do DAG. Se algum modelo ou teste
# falhar, o pipeline para imediatamente.
# ------------------------------------------------------------------------------
echo "[6/7] dbt build --target dev_bq"
dbt build --target dev_bq

# ------------------------------------------------------------------------------
# Etapa 7 — gera target/manifest.json + catalog.json + index.html para o
# lineage. `dbt docs serve` é blocking (sobe servidor HTTP); deixamos para o
# operador rodar manual em outro terminal.
# ------------------------------------------------------------------------------
echo "[7/7] dbt docs generate --target dev_bq"
dbt docs generate --target dev_bq

echo
echo "Pipeline concluído. Para visualizar o lineage:"
echo "  dbt docs serve --port 8080 --target dev_bq"
