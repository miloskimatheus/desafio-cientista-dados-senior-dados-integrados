-- Grão: (aluno_id, bimestre, disciplina).
-- Enriquece  avaliações pivotando disciplinas, adicionando contexto
-- de escola e ano letivo, e identificando alunos multi-escola.
-- Inglês some devido ao `WHERE nota IS NOT NULL`.

{{
    config(
        materialized='view',
        contract={'enforced': true}
    )
}}

with
avaliacao_unpivot as (
    select
        aluno_id,
        turma_id,
        bimestre,
        nota,
        case disciplina_codigo
            when 'disciplina_1' then 'Português'
            when 'disciplina_2' then 'Ciências'
            when 'disciplina_3' then 'Inglês'
            when 'disciplina_4' then 'Matemática'
        end as disciplina

    from {{ ref('stg_educacao__avaliacao') }}
    unpivot (
        nota for disciplina_codigo in (
            disciplina_1, disciplina_2, disciplina_3, disciplina_4
        )
    )
    where nota is not null
),

-- Avaliação não tem `ano_letivo`.
-- Assumo a premissa de 1 turma_id -> 1 ano.
-- QUALIFY garante apenas um ano (menor ano_letivo) por turma id.
turma_ano as (
    select
        turma_id,
        ano_letivo

    from {{ ref('stg_educacao__turma') }}
    where turma_id is not null and ano_letivo is not null
    qualify row_number() over (
        partition by turma_id order by ano_letivo asc
    ) = 1
),

com_escola_e_ano as (
    select
        av.aluno_id,
        av.turma_id,
        av.bimestre,
        av.disciplina,
        av.nota,
        te.escola_id,
        ta.ano_letivo,
        count(distinct te.escola_id) over (
            partition by av.aluno_id, ta.ano_letivo
        ) > 1 as aluno_multi_escola

    from avaliacao_unpivot as av
    left join {{ ref('int_educacao__turma_escola') }} as te
        on av.turma_id = te.turma_id
    left join turma_ano as ta
        on av.turma_id = ta.turma_id
)

select
    -- pk
    aluno_id,
    bimestre,
    disciplina,

    -- fk
    turma_id,
    escola_id,

    -- temporal
    ano_letivo,

    -- métrica
    nota,

    -- flag
    aluno_multi_escola

from com_escola_e_ano
