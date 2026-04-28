{#
    apply_policy_tags
    -----------------
    Aplica em BigQuery os `meta.policy_tags_planned` declarados nas
    colunas dos schema.yml, via `ALTER TABLE ... ALTER COLUMN SET
    OPTIONS (policy_tags = ...)`. Condicionado a
    `var('apply_policy_tags', false)`.

    Uso (post-hook):
        post-hook: "{{ apply_policy_tags() }}"

    Comportamento:
      - `var('apply_policy_tags')` falso: no-op (string vazia).
      - Materialização diferente de `table`: no-op (o BigQuery só
        aceita policy tags em colunas de TABLE).
      - Caso contrário, emite um `ALTER TABLE` por coluna com
        `meta.policy_tags_planned`.

    A habilitação real exige a Data Catalog API, a role
    `roles/datacatalog.policyTagAdmin` e billing, fora do escopo
    custo-zero. Em `dev_bq`, o flag fica `false` por contrato. O macro
    existe para documentar e materializar o caminho de prod sem
    duplicar policy tags em `meta.` e em `+policy_tags`.

    Ref.: BigQuery, column-level access control.
#}
{% macro apply_policy_tags() %}
    {%- if not var('apply_policy_tags', false) -%}
        {#- Flag desligada: no-op em silêncio. -#}
    {%- elif model.config.materialized != 'table' -%}
        {#- Policy tags só valem para TABLE no BigQuery. -#}
    {%- else -%}
        {%- for col_name, col in model.columns.items() -%}
            {%- set planned = col.meta.get('policy_tags_planned') if col.meta else none -%}
            {%- if planned -%}
                alter table {{ this }}
                alter column {{ adapter.quote(col_name) }}
                set options (policy_tags = struct([
                    {%- for tag in planned -%}
                        '{{ tag }}'{% if not loop.last %}, {% endif %}
                    {%- endfor -%}
                ]));
            {% endif -%}
        {%- endfor -%}
    {%- endif -%}
{% endmacro %}
