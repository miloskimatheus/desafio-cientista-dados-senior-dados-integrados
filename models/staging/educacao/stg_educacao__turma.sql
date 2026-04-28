-- 1 linha por (aluno_id, turma_id) - 97.044 linhas.
-- Sem `escola_id`,
-- resolvido em `int_educacao__turma_escola` via `stg_frequencia`.

with
source as (
    select *
    from {{ source('educacao', 'turma') }}
),

renamed as (
    select
        to_hex(id_aluno) as aluno_id,
        safe_cast(id_turma as int64) as turma_id,

        safe_cast(ano as int64) as ano_letivo

    from source
)

select
    -- pk
    aluno_id,
    turma_id,

    -- temporal
    ano_letivo
from renamed
