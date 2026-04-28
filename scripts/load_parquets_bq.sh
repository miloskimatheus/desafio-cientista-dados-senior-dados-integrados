#!/usr/bin/env bash
# ==============================================================================
# load_parquets_bq.sh — Carrega os 4 Parquets em `rmi_educacao_brutos`.
# ------------------------------------------------------------------------------
# Lê os arquivos do bucket público `gs://case_vagas/rmi/` e grava cada um em
# BigQuery com `--replace` (apaga e recria). N execuções produzem o mesmo
# resultado - idempotência via entrada estática (duas execuções produzem
# resultado idêntico).
#
# Os arquivos no bucket NÃO têm extensão `.parquet`: o conteúdo é Parquet, mas
# o nome é só `aluno`, `turma`, `frequencia`, `avaliacao`. `bq load` infere o
# formato pelo flag `--source_format=PARQUET`, não pela extensão.
#
# Carrega só 4 das 5 tabelas: `escola` é ignorado porque nenhum modelo dbt o
# consome — `escola_id` é recuperado via `stg_educacao__frequencia` em
# `int_educacao__turma_escola`, e `bairro` é HASH com salt (não-joinável).
# Carregar `escola` desperdiçaria storage e criaria tabela órfã.
#
# Pré-requisito: ./scripts/setup_gcp.sh (cria o dataset).
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# Configuração: o projeto vem do ambiente; o resto é fixo por design.
# ------------------------------------------------------------------------------
PROJECT_ID="${DBT_GCP_PROJECT:?defina DBT_GCP_PROJECT antes de rodar}"
RAW_DATASET="rmi_educacao_brutos"
BUCKET_URI="gs://case_vagas/rmi"
LOCATION="US"

# 4 tabelas declaradas em `_educacao__sources.yml` (sem `escola`).
TABELAS=(aluno turma frequencia avaliacao)

log() { echo "[load_parquets] $*" >&2; }

# ------------------------------------------------------------------------------
# Pré-requisitos: SDK + dataset criado pelo setup_gcp.sh.
# ------------------------------------------------------------------------------
for cmd in bq gsutil; do
  command -v "$cmd" >/dev/null \
    || { log "FAIL: '$cmd' ausente. Instale o Google Cloud SDK."; exit 1; }
done

if ! bq --project_id="${PROJECT_ID}" show "${RAW_DATASET}" >/dev/null 2>&1; then
  log "FAIL: dataset ${RAW_DATASET} não existe. Rode ./scripts/setup_gcp.sh primeiro."
  exit 1
fi

log "projeto=${PROJECT_ID} dataset=${RAW_DATASET} location=${LOCATION}"

# ------------------------------------------------------------------------------
# Loop principal: carrega cada tabela.
#   --source_format=PARQUET → schema vem do próprio arquivo (tipos embarcados).
#   --replace               → apaga e recria a tabela (idempotência).
# Flag global `--quiet` no `bq` esconde as linhas "Waiting on bqjob..."
# (mantém erros reais); nossas linhas de log já dizem o que interessa.
# ------------------------------------------------------------------------------
for tabela in "${TABELAS[@]}"; do
  origem="${BUCKET_URI}/${tabela}"
  destino="${PROJECT_ID}:${RAW_DATASET}.${tabela}"
  log "-> ${tabela}: ${origem}"
  bq --quiet --project_id="${PROJECT_ID}" --location="${LOCATION}" load \
     --source_format=PARQUET \
     --replace \
     "${destino}" \
     "${origem}"
done

log "OK — 4 tabelas em ${RAW_DATASET}."
