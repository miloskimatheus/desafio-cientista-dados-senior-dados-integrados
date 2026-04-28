-- Mart principal: efeito-escola por (escola, disciplina, ano_letivo).
-- Grão: PK composta (~31k linhas em prod com 5 anos; 1.347 em dev-sample).
-- NULLIF protege divisões; DENSE_RANK evita buracos em empates.
{{
    config(
        materialized='table',
        partition_by={
            'field': 'ano_letivo',
            'data_type': 'int64',
            'range': {'start': 2000, 'end': 2100, 'interval': 1}
        },
        cluster_by=['disciplina', 'classificacao', 'escola_id'],
        contract={'enforced': true}
    )
}}

with
-- lê o efeito bruto, n_obs e pct_obs_multi_escola por (escola, disciplina, ano)
bruto as (
    select
        escola_id,
        disciplina,
        ano_letivo,
        efeito_bruto,
        n_obs,
        pct_obs_multi_escola
    from {{ ref('int_educacao__efeito_bruto_por_escola_disciplina_ano') }}
),

-- junta variâncias por (disciplina, ano) e aplica GREATEST(0, var_between)
-- para preservar not_null de peso_shrinkage e erro_padrao
com_variancias as (
    select
        b.escola_id,
        b.disciplina,
        b.ano_letivo,
        b.efeito_bruto,
        b.n_obs,
        b.pct_obs_multi_escola,
        v.var_within,
        greatest(0, v.var_between) as var_between_clamped

    from bruto as b
    left join {{ ref('int_educacao__variancia_por_disciplina') }} as v
        on b.disciplina = v.disciplina and b.ano_letivo = v.ano_letivo
),

-- calcula peso_shrinkage = var_between / (var_between + var_within / n_obs)
-- e o efeito ajustado (peso x efeito_bruto)
com_shrinkage as (
    select
        escola_id,
        disciplina,
        ano_letivo,
        efeito_bruto,
        n_obs,
        pct_obs_multi_escola,
        var_between_clamped,
        var_within,

        var_between_clamped
        / nullif(var_between_clamped + var_within / n_obs, 0)
            as peso_shrinkage,

        (
            var_between_clamped
            / nullif(var_between_clamped + var_within / n_obs, 0)
        ) * efeito_bruto
            as efeito_ajustado

    from com_variancias
),

-- erro padrão do efeito ajustado: sqrt((1 - peso_shrinkage) x var_between)
com_erro_padrao as (
    select
        escola_id,
        disciplina,
        ano_letivo,
        efeito_bruto,
        n_obs,
        pct_obs_multi_escola,
        peso_shrinkage,
        efeito_ajustado,
        sqrt((1 - peso_shrinkage) * var_between_clamped) as erro_padrao
    from com_shrinkage
),

-- monta saída: IC 95%, classificação por magnitude, confiança estatística
-- (IC cruza 0?) e ranking dentro de (disciplina, ano)
final as (
    select
        escola_id,
        disciplina,
        ano_letivo,
        n_obs,
        efeito_bruto,
        peso_shrinkage,
        efeito_ajustado,
        erro_padrao,
        efeito_ajustado - 1.96 * erro_padrao as ic_inferior,
        efeito_ajustado + 1.96 * erro_padrao as ic_superior,

        case
            when efeito_ajustado >= {{ var('classificacao_corte_forte') }}
                then 'forte_positivo'
            when efeito_ajustado > {{ var('classificacao_corte_fraco') }}
                then 'moderado_positivo'
            when efeito_ajustado >= -{{ var('classificacao_corte_fraco') }}
                then 'indistinguivel'
            when efeito_ajustado > -{{ var('classificacao_corte_forte') }}
                then 'moderado_negativo'
            else 'forte_negativo'
        end as classificacao,

        case
            when
                (efeito_ajustado - 1.96 * erro_padrao) > 0
                or (efeito_ajustado + 1.96 * erro_padrao) < 0
                then 'significativo'
            else 'inconclusivo'
        end as confianca_estatistica,

        pct_obs_multi_escola,

        dense_rank() over (
            partition by disciplina, ano_letivo
            order by efeito_ajustado desc
        ) as rank_na_disciplina

    from com_erro_padrao
)

select
    -- pk
    escola_id,
    disciplina,
    ano_letivo,

    -- cobertura
    n_obs,
    pct_obs_multi_escola,

    -- métrica bruta
    efeito_bruto,

    -- métricas ajustadas
    peso_shrinkage,
    efeito_ajustado,
    erro_padrao,
    ic_inferior,
    ic_superior,

    -- variáveis para consumo
    classificacao,
    confianca_estatistica,

    -- ranking da escola dentro da disciplina
    -- (em determinado ano_letivo)
    rank_na_disciplina

from final
