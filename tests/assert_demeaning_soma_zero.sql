-- Regra: para todo (aluno, ano_letivo) em
-- `int_educacao__notas_demeanadas`, `ABS(AVG(nota_demeanada)) <= 1e-6`.
-- É uma propriedade algébrica do demeaning: subtrair a média do aluno
-- no ano faz a soma dos resíduos do aluno naquele ano ser zero.
-- Por que importa: o mart trata `nota_demeanada` como desvio puro do
-- padrão intra-aluno-ano. Se a soma não for zero, o `efeito_bruto`
-- herda viés e a comparação entre escolas perde sentido.
-- Gatilho de ação: revisar a window function `AVG(nota) OVER
-- (PARTITION BY aluno_id, ano_letivo)` em
-- `int_educacao__notas_demeanadas`; confirmar que `nota IS NOT NULL`
-- é consistente entre numerador e denominador da média e que
-- `ano_letivo` não está propagando NULL.
{{
    config(
        severity='error'
    )
}}

with
soma_por_aluno_ano as (
    select
        aluno_id,
        ano_letivo,
        avg(nota_demeanada) as media_demeanada_aluno_ano
    from {{ ref('int_educacao__notas_demeanadas') }}
    group by aluno_id, ano_letivo
)

select
    aluno_id,
    ano_letivo,
    media_demeanada_aluno_ano
from soma_por_aluno_ano
where abs(media_demeanada_aluno_ano) > 1e-6
