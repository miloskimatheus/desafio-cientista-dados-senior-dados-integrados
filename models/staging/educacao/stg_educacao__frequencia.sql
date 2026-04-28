-- pós dedup 1 linha por (aluno_id, turma_id, disciplina, frequencia_data_inicio).
-- 2.725M linhas (~88% do storage) após resolver 2.556 grupos de
-- duplicatas via `QUALIFY ROW_NUMBER()`.
-- Única staging com `(turma_id, escola_id)` juntos.
-- fonte para `int_turma_escola`.

with
source as (
    select *
    from {{ source('educacao', 'frequencia') }}
),

renamed as (
    select
        to_hex(id_aluno) as aluno_id,
        safe_cast(id_turma as int64) as turma_id,
        trim(disciplina) as disciplina,
        safe.parse_date('%Y-%m-%d', data_inicio) as frequencia_data_inicio,
        safe_cast(id_escola as int64) as escola_id,

        safe.parse_date('%Y-%m-%d', data_fim) as frequencia_data_fim,

        round(safe_cast(frequencia as float64), 2) as frequencia_percentual

    from source

    qualify row_number() over (
        partition by aluno_id, turma_id, disciplina, frequencia_data_inicio
        order by
            frequencia_percentual asc nulls last,
            frequencia_data_fim asc nulls last,
            escola_id asc nulls last
    ) = 1
)

select
    -- pk
    aluno_id,
    turma_id,
    disciplina,
    frequencia_data_inicio,

    -- fk
    escola_id,

    -- temporal
    frequencia_data_fim,

    -- métrica
    frequencia_percentual
from renamed
