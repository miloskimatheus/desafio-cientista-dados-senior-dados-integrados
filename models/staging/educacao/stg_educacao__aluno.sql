-- 1 linha por aluno (97.044). Cadastro anonimizado.
-- `aluno_id` STRING via `TO_HEX(BYTES)`
-- `aluno_bairro` é HASH, não-joinável com `datario`

with
source as (
    select *
    from {{ source('educacao', 'aluno') }}
),

renamed as (
    select
        to_hex(id_aluno) as aluno_id,
        safe_cast(id_turma as int64) as turma_id,
        trim(faixa_etaria) as faixa_etaria,
        safe_cast(bairro as int64) as aluno_bairro

    from source
)

select
    -- pk
    aluno_id,

    -- fk
    turma_id,

    -- demográficos
    faixa_etaria,
    aluno_bairro
from renamed
