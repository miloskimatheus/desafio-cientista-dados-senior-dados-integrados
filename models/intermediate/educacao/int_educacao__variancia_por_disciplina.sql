-- Grão: (disciplina, ano_letivo) - 1 linha por par.
-- Calcula variâncias e tamanho efetivo da amostra.
-- Filtro `escola_id IS NOT NULL` elimina ~3,83% órfãos em dev.
{{
    config(
        materialized='view',
        contract={'enforced': true}
    )
}}

with
-- lê notas demeanadas, descarta observações sem escola (órfãos)
notas as (
    select
        escola_id,
        disciplina,
        ano_letivo,
        nota_demeanada
    from {{ ref('int_educacao__notas_demeanadas') }}
    where escola_id is not null
),

-- efeito bruto e o tamanho da amostra de cada (escola, disciplina, ano)
por_escola_disciplina_ano as (
    select
        escola_id,
        disciplina,
        ano_letivo,
        efeito_bruto as efeito_bruto_escola,
        n_obs as n_obs_escola
    from {{ ref('int_educacao__efeito_bruto_por_escola_disciplina_ano') }}
),

-- soma dos quadrados intra-escola: dispersão das notas em torno do efeito bruto da própria escola
ss_within as (
    select
        n.disciplina,
        n.ano_letivo,
        sum(power(n.nota_demeanada - p.efeito_bruto_escola, 2)) as ss_within
    from notas as n
    inner join por_escola_disciplina_ano as p
        on
            n.escola_id = p.escola_id
            and n.disciplina = p.disciplina
            and n.ano_letivo = p.ano_letivo
    group by n.disciplina, n.ano_letivo
),

-- agrega por (disciplina, ano): totais, graus de liberdade, variância dos
-- efeitos brutos e tamanho efetivo n_eff
agregados_por_disciplina_ano as (
    select
        disciplina,
        ano_letivo,
        sum(n_obs_escola) as n_total,
        count(distinct escola_id) as n_escolas,
        sum(n_obs_escola) - count(distinct escola_id) as df_within,
        var_samp(efeito_bruto_escola) as var_efeitos_brutos,
        (
            sum(n_obs_escola) - sum(n_obs_escola * n_obs_escola) * 1.0
            / nullif(sum(n_obs_escola), 0)
        )
        / nullif(count(distinct escola_id) - 1, 0) as n_eff
    from por_escola_disciplina_ano
    group by disciplina, ano_letivo
),

-- variância total das notas demeanadas por (disciplina, ano)
variancia_total as (
    select
        disciplina,
        ano_letivo,
        var_samp(nota_demeanada) as var_total
    from notas
    group by disciplina, ano_letivo
),

-- decompõe variância em var_within (ss_within / df_within) e var_between
-- (método dos momentos sem floor).
componentes as (
    select
        a.disciplina,
        a.ano_letivo,
        v.var_total,
        a.var_efeitos_brutos,
        a.n_eff,
        ssw.ss_within / nullif(a.df_within, 0) as var_within,

        a.var_efeitos_brutos
        - (ssw.ss_within / nullif(a.df_within, 0)) / nullif(a.n_eff, 0)
            as var_between

    from agregados_por_disciplina_ano as a
    inner join variancia_total as v
        on a.disciplina = v.disciplina and a.ano_letivo = v.ano_letivo
    inner join ss_within as ssw
        on a.disciplina = ssw.disciplina and a.ano_letivo = ssw.ano_letivo
)

select
    -- pk
    disciplina,
    ano_letivo,

    -- variancias
    var_total,
    var_within,
    var_between,

    -- tamanho efetivo da amostra
    n_eff

from componentes
