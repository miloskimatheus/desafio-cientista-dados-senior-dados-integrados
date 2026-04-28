-- Regra: a fração de linhas em `int_educacao__avaliacao_enriquecida`
-- com `escola_id IS NULL` (o LEFT JOIN com
-- `int_educacao__turma_escola` não casou) fica abaixo de
-- `var('max_pct_orfaos_avaliacao_turma')`.
-- Por que importa: avaliações sem escola atribuída ficam de fora dos
-- agregados do mart. Fora de `prod_bq`, severidade `warn` registra o
-- gap sem bloquear o build; em `prod_bq`, vira `error`.
-- Gatilho de ação: em prod, investigar os `turma_id` órfãos; se for
-- sistemático, ajustar a ingestão de `turma` e `frequencia`.
{{
    config(
        severity="error" if target.name == "prod_bq" else "warn"
    )
}}

with cobertura as (
    select
        countif(escola_id is null) as n_orfaos,
        count(*) as n_total
    from {{ ref('int_educacao__avaliacao_enriquecida') }}
)

select
    n_orfaos,
    n_total,
    safe_divide(n_orfaos, n_total) as pct_orfaos
from cobertura
where
    safe_divide(n_orfaos, n_total)
    > {{ var('max_pct_orfaos_avaliacao_turma') }}
