-- Sentinela do `apply_policy_tags`.
-- Regra: quando `var('apply_policy_tags') == false` (default
-- custo-zero, Sandbox sem billing), o teste passa de forma trivial e
-- confirma que ligar o flag em prod não passa em silêncio. Quando o
-- flag está em `true`, garante que os FQNs em `dbt_project.yml/vars`
-- (`policy_tag_identificador_direto`, `policy_tag_demografico`,
-- `policy_tag_dados_de_menores_desempenho`) não contêm os
-- placeholders `PROJECT_ID` ou `TAXONOMY_ID`.
-- Por que importa: o macro `apply_policy_tags()` está registrado como
-- post-hook global. Sem a sentinela, ligar o flag em prod pode aplicar
-- tags com FQN inválido (o Data Catalog rejeita em alguns
-- casos) ou nenhum tag, e o CI/CD não detecta.
-- Gatilho de ação (quando o flag em `true` falhar): substituir os
-- placeholders `PROJECT_ID` e `TAXONOMY_ID` em `dbt_project.yml/vars`
-- pelos IDs reais da taxonomia, provisionada via Terraform ou gcloud.
-- Verificar que a Service Account em uso tem
-- `roles/datacatalog.policyTagAdmin`.
{{
    config(
        severity="error"
    )
}}

{%- set flag_on = var('apply_policy_tags', false) -%}

{%- if not flag_on -%}
    -- Caminho default (Sandbox, custo-zero): sentinela passa com
    -- resultset vazio. Usamos `UNNEST(ARRAY<INT64>[])` porque o
    -- BigQuery rejeita `WHERE` sem `FROM`.
    select
        cast(null as string) as motivo,
        cast(null as string) as fqn
    from unnest(cast([] as array<int64>)) as x
{%- else -%}
    -- Caminho de prod (flag `true`): cada FQN com placeholder vira
    -- uma linha de erro.
    select * from unnest([
        struct(
            'placeholder PROJECT_ID/TAXONOMY_ID em FQN' as motivo,
            '{{ var("policy_tag_identificador_direto") }}' as fqn
        ),
        struct(
            'placeholder PROJECT_ID/TAXONOMY_ID em FQN' as motivo,
            '{{ var("policy_tag_demografico") }}' as fqn
        ),
        struct(
            'placeholder PROJECT_ID/TAXONOMY_ID em FQN' as motivo,
            '{{ var("policy_tag_dados_de_menores_desempenho") }}' as fqn
        )
    ])
    where regexp_contains(fqn, r'PROJECT_ID|TAXONOMY_ID')
       or fqn = ''
{%- endif -%}
