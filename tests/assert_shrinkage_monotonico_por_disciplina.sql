-- Regra: dentro de cada (disciplina, ano_letivo) em
-- `mart_educacao__efeito_diferencial_disciplina`, ao ordenar escolas
-- por `n_obs` crescente, `peso_shrinkage` não pode diminuir. Ou seja,
-- `peso_shrinkage(linha N) >= peso_shrinkage(linha N-1) - 1e-6`.
-- Por que importa: na fórmula
-- `var_between / (var_between + var_within / n_obs)`, `var_between`
-- e `var_within` são constantes por (disciplina, ano_letivo). O único
-- termo que muda entre escolas naquele recorte é `n_obs`: quando
-- `n_obs` cresce, `var_within / n_obs` diminui e o peso aumenta. Se
-- a sequência quebrar, há bug na fórmula ou no JOIN com
-- `int_educacao__variancia_por_disciplina`. A tolerância 1e-6 cobre
-- arredondamento FLOAT64.
-- Gatilho de ação: revisar a CTE `com_shrinkage` do mart; confirmar
-- que `var_between` e `var_within` vêm do intermediate de variâncias
-- via `LEFT JOIN ... USING (disciplina, ano_letivo)` e que `n_obs` é
-- o agregado por (escola, disciplina, ano_letivo), não global.
{{
    config(
        severity='error'
    )
}}

with
ordenado as (
    select
        disciplina,
        ano_letivo,
        escola_id,
        n_obs,
        peso_shrinkage,
        lag(peso_shrinkage) over (
            partition by disciplina, ano_letivo
            order by n_obs asc, escola_id asc
        ) as peso_shrinkage_anterior
    from {{ ref('mart_educacao__efeito_diferencial_disciplina') }}
)

select
    disciplina,
    ano_letivo,
    escola_id,
    n_obs,
    peso_shrinkage,
    peso_shrinkage_anterior
from ordenado
where
    peso_shrinkage_anterior is not null
    and peso_shrinkage < peso_shrinkage_anterior - 1e-6
