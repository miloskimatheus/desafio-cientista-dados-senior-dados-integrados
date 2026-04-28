-- Regra: para cada (aluno_id, ano_letivo, disciplina) em
-- `int_educacao__avaliacao_enriquecida`, os bimestres observados
-- formam uma sequência contínua a partir de 1, ou seja,
-- `count(distinct bimestre) = max(bimestre)`.
-- Por que importa: bimestre é o eixo temporal. Uma nota em B1 e B3
-- sem B2 aponta problema na ingestão ou no dedup. O demeaning por
-- (aluno, ano_letivo) tolera lacunas erroneamente; sem este teste, a
-- falha enviesa o `efeito_ajustado` no mart.
-- Gatilho de ação: localizar a tupla que falhou; verificar se o
-- bimestre ausente é NULL legítimo (transferência durante o ano) ou
-- se a linha foi descartada upstream (dedup ou filtro
-- `nota IS NOT NULL` na CTE `avaliacao_unpivot`). Fora de `prod_bq`,
-- severidade `warn` (a amostra pode pular bimestre por acaso); em
-- `prod_bq`, `error` é o gate.
{{
    config(
        severity="error" if target.name == "prod_bq" else "warn"
    )
}}

with
bimestres_por_aluno as (
    select
        aluno_id,
        ano_letivo,
        disciplina,
        count(distinct bimestre) as n_bimestres,
        max(bimestre) as bimestre_max
    from {{ ref('int_educacao__avaliacao_enriquecida') }}
    where bimestre is not null
    group by aluno_id, ano_letivo, disciplina
)

select
    aluno_id,
    ano_letivo,
    disciplina,
    n_bimestres,
    bimestre_max
from bimestres_por_aluno
where n_bimestres != bimestre_max
