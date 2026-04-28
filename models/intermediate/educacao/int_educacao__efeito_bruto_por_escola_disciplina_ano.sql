-- Grão: (escola_id, disciplina, ano_letivo).
-- Calcula o efeito bruto médio de notas por escola, disciplina e ano letivo.
-- Filtro `escola_id IS NOT NULL` elimina ~3,83% órfãos em dev.

{{
    config(
        materialized='view',
        contract={'enforced': true}
    )
}}

with
notas as (
    select
        escola_id,
        disciplina,
        ano_letivo,
        nota_demeanada,
        aluno_multi_escola

    from {{ ref('int_educacao__notas_demeanadas') }}
    where escola_id is not null
),

agregado as (
    select
        escola_id,
        disciplina,
        ano_letivo,

        avg(nota_demeanada) as efeito_bruto,
        count(*) as n_obs,
        avg(
            case
                when aluno_multi_escola then 1.0
                else 0.0
            end
        ) as pct_obs_multi_escola

    from notas
    group by escola_id, disciplina, ano_letivo
)

select
    -- pk
    escola_id,
    disciplina,
    ano_letivo,

    -- cobertura
    n_obs,
    pct_obs_multi_escola,

    -- métrica
    efeito_bruto

from agregado
