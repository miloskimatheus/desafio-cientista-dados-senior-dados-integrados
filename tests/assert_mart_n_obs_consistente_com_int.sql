-- Regra: para cada (disciplina, ano_letivo), `SUM(n_obs)` no mart
-- é igual a `COUNT(*)` em `int_educacao__notas_demeanadas` no mesmo
-- recorte. Ou seja, nenhuma linha do intermediate é perdida ou
-- duplicada na cadeia `int_notas_demeanadas` -> `int_efeito_bruto_*`
-- -> mart.
-- Por que importa: valida o lineage de ponta a ponta. Sem este teste,
-- um bug nos joins ou agrupamentos intermediários (filtro,
-- `INNER` virando `LEFT` por engano, chave de agregação errada) passa
-- despercebido. O mart fica plausível e os testes por modelo passam,
-- mas a contagem total não bate com o que entrou no demeaning.
-- Severidade `error` em qualquer target: propriedade algébrica.
-- Gatilho de ação: localizar a (disciplina, ano_letivo) divergente;
-- inspecionar `int_educacao__efeito_bruto_por_escola_disciplina_ano`
-- (`COUNT(*)` e `AVG(...)` por escola, disciplina e ano), o filtro
-- `escola_id IS NOT NULL` na primeira CTE, e o `LEFT JOIN` entre
-- mart e variâncias.
{{
    config(
        severity="error"
    )
}}

with
mart_agg as (
    select
        disciplina,
        ano_letivo,
        sum(n_obs) as n_obs_mart
    from {{ ref('mart_educacao__efeito_diferencial_disciplina') }}
    group by disciplina, ano_letivo
),

int_agg as (
    -- O mart só carrega tuplas com `escola_id IS NOT NULL` (filtro
    -- em `int_efeito_bruto_*`); espelhamos aqui para casar a contagem.
    select
        disciplina,
        ano_letivo,
        count(*) as n_obs_int
    from {{ ref('int_educacao__notas_demeanadas') }}
    where escola_id is not null
    group by disciplina, ano_letivo
),

comparado as (
    select
        m.n_obs_mart,
        i.n_obs_int,
        coalesce(m.disciplina, i.disciplina) as disciplina,
        coalesce(m.ano_letivo, i.ano_letivo) as ano_letivo
    from mart_agg as m
    full outer join int_agg as i
        on
            m.disciplina = i.disciplina
            and m.ano_letivo = i.ano_letivo
)

select *
from comparado
where coalesce(n_obs_mart, -1) != coalesce(n_obs_int, -1)
