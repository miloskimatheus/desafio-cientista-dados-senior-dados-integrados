-- Roteamento de schema por camada (pass-through).
--
-- Comportamento:
-- 1. Sem `custom_schema_name`: cai no schema default do target
--    (padrão do dbt).
-- 2. Com `custom_schema_name` (staging, intermediate ou marts via
--    `+schema:` no `dbt_project.yml`): retorna o nome completo do
--    dataset, sem prefixar com `target.schema`. Os valores em
--    `dbt_project.yml` já são os nomes finais (ex.:
--    `rmi_educacao_staging`). Sem este macro, o dbt geraria
--    `<schema>_rmi_educacao_staging`.
--
-- Em produção real, a separação dev/prod é por projeto GCP (billing
-- isolado), não por prefixo de dataset no mesmo Sandbox.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
