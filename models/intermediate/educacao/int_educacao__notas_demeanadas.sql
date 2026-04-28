-- Grão: (aluno_id, turma_id, ano_letivo, bimestre, disciplina).
-- Gera notas "demeneadas" (diferença contra a média)
{{
    config(
        materialized='table',
        partition_by={
            'field': 'ano_letivo',
            'data_type': 'int64',
            'range': {'start': 2000, 'end': 2100, 'interval': 1}
        },
        contract={'enforced': true}
    )
}}

with
fonte as (
    select
        aluno_id,
        turma_id,
        escola_id,
        ano_letivo,
        bimestre,
        disciplina,
        nota,
        aluno_multi_escola
    from {{ ref('int_educacao__avaliacao_enriquecida') }}
    where ano_letivo is not null
),

notas as (
    select
        aluno_id,
        turma_id,
        escola_id,
        ano_letivo,
        bimestre,
        disciplina,
        nota,
        aluno_multi_escola,

        avg(nota) over (
            partition by aluno_id, ano_letivo
        ) as media_aluno,

        nota - avg(nota) over (
            partition by aluno_id, ano_letivo
        ) as nota_demeanada,

        count(*) over (
            partition by aluno_id, ano_letivo
        ) as n_obs_aluno,

        count(distinct disciplina) over (
            partition by aluno_id, ano_letivo
        ) as n_disc_aluno

    from fonte
    qualify
        n_obs_aluno >= {{ var('min_obs_por_aluno') }}
        and n_disc_aluno >= 2
)

select
    -- pk
    aluno_id,
    turma_id,
    ano_letivo,
    bimestre,
    disciplina,

    -- fk
    escola_id,

    -- métrica original
    nota,

    -- métricas derivadas
    media_aluno,
    nota_demeanada,
    n_obs_aluno,

    -- flag
    aluno_multi_escola

from notas
