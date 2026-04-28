-- pós-dedup 1 linha por (aluno_id, bimestre)
-- Wide: 4 colunas `disciplina_N`.
-- 581 colisões (0,26%) resolvidas via `QUALIFY ROW_NUMBER()`
-- `disciplina_3` (Inglês) 100% NULL no dev-sample

with
source as (
    select *
    from {{ source('educacao', 'avaliacao') }}
),

renamed as (
    select
        to_hex(id_aluno) as aluno_id,
        safe_cast(bimestre as int64) as bimestre,
        safe_cast(id_turma as int64) as turma_id,

        round(safe_cast(disciplina_1 as float64), 2) as disciplina_1,
        round(safe_cast(disciplina_2 as float64), 2) as disciplina_2,
        round(safe_cast(disciplina_3 as float64), 2) as disciplina_3,
        round(safe_cast(disciplina_4 as float64), 2) as disciplina_4

    from source

    qualify row_number() over (
        partition by aluno_id, bimestre
        order by
            turma_id asc nulls last,
            disciplina_1 asc nulls last,
            disciplina_2 asc nulls last,
            disciplina_3 asc nulls last,
            disciplina_4 asc nulls last
    ) = 1
)

select
    -- pk
    aluno_id,
    bimestre,

    -- fk
    turma_id,

    -- métricas
    disciplina_1,
    disciplina_2,
    disciplina_3,
    disciplina_4
from renamed
