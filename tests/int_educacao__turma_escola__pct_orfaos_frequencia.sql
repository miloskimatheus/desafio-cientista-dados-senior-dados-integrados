-- Regra: a fração de turmas em `stg_educacao__frequencia` cujo
-- `escola_id` é NULL em todas as linhas (e por isso não entram em
-- `int_educacao__turma_escola`) fica abaixo de
-- `var('max_pct_orfaos_frequencia_turma')`.
-- Por que importa: `int_educacao__turma_escola` filtra
-- `escola_id IS NOT NULL` na CTE de origem. Turmas que só aparecem em
-- `frequencia` com `escola_id` NULL saem da resolução turma para
-- escola e propagam o NULL para `int_educacao__avaliacao_enriquecida`,
-- ficando de fora dos agregados do mart.
-- Gatilho de ação: investigar essas turmas; se for sistemático,
-- ajustar a ingestão de `frequencia`.
{{
    config(
        severity='error'
        )
}}

with
por_turma as (
    select
        turma_id,
        max(
            case when escola_id is not null then 1 else 0 end
        ) as tem_escola
    from {{ ref('stg_educacao__frequencia') }}
    where turma_id is not null
    group by turma_id
),

cobertura as (
    select
        countif(tem_escola = 0) as n_orfaos,
        count(*) as n_total
    from por_turma
)

select
    n_orfaos,
    n_total,
    safe_divide(n_orfaos, n_total) as pct_orfaos
from cobertura
where
    safe_divide(n_orfaos, n_total)
    > {{ var('max_pct_orfaos_frequencia_turma') }}
