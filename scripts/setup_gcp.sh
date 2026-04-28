#!/usr/bin/env bash
# ==============================================================================
# setup_gcp.sh — Prepara o projeto GCP Sandbox para receber os Parquets.
# ------------------------------------------------------------------------------
# Em ordem:
#   V0  billing NÃO vinculada (Sandbox = custo zero).
#   V1  `datario.dados_mestres.bairro` em US (BigQuery não permite JOIN
#        cross-region; nossos datasets também precisam estar em US).
#   V2  acesso ao bucket público dos Parquets.
#   1   habilita APIs de BigQuery + Cloud Storage.
#   2   cria os 4 datasets em US (idempotente).
#
# As validações V0/V1/V2 vêm antes de criar recurso: se algo falhar, paramos
# sem ter sujado o projeto.
#
# Pré-requisitos (manuais):
#   gcloud auth application-default login
#   export DBT_GCP_PROJECT=<seu-projeto-sandbox>
# ==============================================================================

# `-e` aborta no primeiro erro; `-u` falha se var não definida; `pipefail` faz
# `cmd1 | cmd2` falhar se `cmd1` falhar (por default só o exit de `cmd2` conta).
set -euo pipefail

# ------------------------------------------------------------------------------
# Configuração: o projeto vem do ambiente; o resto é fixo por design.
# ------------------------------------------------------------------------------
PROJECT_ID="${DBT_GCP_PROJECT:?defina DBT_GCP_PROJECT antes de rodar}"
LOCATION="US"
BUCKET_URI="gs://case_vagas/rmi/"
DATARIO_TABLE="datario:dados_mestres.bairro"

# Os 4 datasets do DAG: brutos (saída do bq load) + as 3 camadas dbt.
DATASETS=(
  rmi_educacao_brutos
  rmi_educacao_staging
  rmi_educacao_intermediate
  rmi_educacao_marts
)

# Logs vão para stderr; deixa stdout livre para saída programática futura.
log() { echo "[setup_gcp] $*" >&2; }

# ------------------------------------------------------------------------------
# Pré-requisitos: Google Cloud SDK presente no PATH.
# ------------------------------------------------------------------------------
for cmd in gcloud bq gsutil; do
  command -v "$cmd" >/dev/null \
    || { log "FAIL: '$cmd' ausente. Instale o Google Cloud SDK."; exit 1; }
done
log "projeto=${PROJECT_ID} location=${LOCATION}"

# Garante que o `gcloud` aponta para o projeto certo (evita surpresa caso o
# default do SDK esteja em outro projeto). `--no-user-output-enabled` silencia
# o "Updated property" — falha real ainda propaga via exit code (set -e).
gcloud config set project "${PROJECT_ID}" --quiet --no-user-output-enabled

# ------------------------------------------------------------------------------
# V0 — billing desvinculada. Se alguém vincular billing, o Sandbox morre e o
# projeto passa a poder ser cobrado. Este gate é o seguro contra o acidente.
# ------------------------------------------------------------------------------
log "V0: billing desvinculada?"
billing_enabled=$(gcloud beta billing projects describe "${PROJECT_ID}" \
                    --format='value(billingEnabled)' 2>/dev/null || true)
if [[ "${billing_enabled}" == "True" ]]; then
  log "FAIL V0: ${PROJECT_ID} tem billing vinculada."
  log "  Desvincule: gcloud beta billing projects unlink ${PROJECT_ID}"
  exit 1
fi
log "V0 OK."

# ------------------------------------------------------------------------------
# V1 — datario em US. Se mudar de região, o LOCATION acima precisa migrar
# junto, senão JOIN cross-region quebra.
#
# Nota: o CLI `bq` não aceita `--format=value(...)` (só o `gcloud` aceita).
# Os formatos válidos são {none|json|prettyjson|csv|sparse|pretty}. Extraímos
# `location` via grep+sed em cima do prettyjson — evita depender de `jq` (que
# não está no CI default do GitHub Actions).
# ------------------------------------------------------------------------------
log "V1: ${DATARIO_TABLE} em US?"
datario_location=$(bq show --format=prettyjson "${DATARIO_TABLE}" 2>/dev/null \
                    | grep -o '"location": *"[^"]*"' | head -1 \
                    | sed 's/.*"\([^"]*\)"$/\1/')
if [[ "${datario_location}" != "US" ]]; then
  log "FAIL V1: location='${datario_location:-vazio}', esperado US."
  log "  Cheque 'gcloud auth application-default login' e tente de novo."
  exit 1
fi
log "V1 OK."

# ------------------------------------------------------------------------------
# V2 — acesso ao bucket. Sem isso, load_parquets_bq.sh falha logo depois.
# ------------------------------------------------------------------------------
log "V2: acesso a ${BUCKET_URI}?"
if ! gsutil ls "${BUCKET_URI}" >/dev/null 2>&1; then
  log "FAIL V2: sem acesso a ${BUCKET_URI}."
  log "  Confirme em 'gcloud auth list' que a conta ativa é a registrada"
  log "  junto aos organizadores do desafio."
  exit 1
fi
log "V2 OK."

# ------------------------------------------------------------------------------
# Habilita APIs (idempotente: API já habilitada vira no-op).
# `--no-user-output-enabled` silencia a mensagem "Operation ... finished
# successfully"; erros reais ainda derrubam o script via exit code.
# ------------------------------------------------------------------------------
log "habilitando APIs (bigquery, storage)..."
gcloud services enable bigquery.googleapis.com storage.googleapis.com \
  --project="${PROJECT_ID}" --quiet --no-user-output-enabled

# ------------------------------------------------------------------------------
# Cria os 4 datasets em US (existe → no-op; não existe → cria).
# ------------------------------------------------------------------------------
log "criando datasets em ${LOCATION}..."
for ds in "${DATASETS[@]}"; do
  if bq --project_id="${PROJECT_ID}" show "${ds}" >/dev/null 2>&1; then
    log "  ${ds} ok."
  else
    bq --project_id="${PROJECT_ID}" --location="${LOCATION}" \
       mk --dataset "${PROJECT_ID}:${ds}" >/dev/null
    log "  ${ds} criado."
  fi
done

log "OK — pronto para load_parquets_bq.sh."
