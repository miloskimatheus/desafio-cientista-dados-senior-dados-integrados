-- Grão: 1 linha por turma
-- Gera relação canônica de turma_id, escola_id
{{
    config(
        materialized='view',
        contract={'enforced': true}
    )
}}

with
pares_distintos as (
    select distinct
        turma_id,
        escola_id

    from {{ ref('stg_educacao__frequencia') }}
    where
        turma_id is not null
        and escola_id is not null
)

select
    -- pk
    turma_id,

    -- fk
    escola_id

from pares_distintos
