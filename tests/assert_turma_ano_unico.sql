-- Regra: cada `turma_id` em `stg_educacao__turma` tem exatamente um
-- `ano_letivo` distinto. Ou seja, o grão `(turma_id, ano_letivo)` é
-- funcional em `turma_id` (uma turma para um ano).
-- Por que importa: a CTE `turma_ano` em
-- `int_educacao__avaliacao_enriquecida` resolve
-- `turma_id` para `ano_letivo` via `QUALIFY ROW_NUMBER() OVER
-- (PARTITION BY turma_id ORDER BY ano_letivo ASC) = 1`. Se a premissa
-- quebrar (turma_id em vários anos por reuso de id ou turma com
-- vários anos), o desempate "menor ano vence" esconde o bug: a
-- avaliação fica atribuída ao primeiro ano em que a turma aparece,
-- não ao correto. No sample de dev, o ano é fixo em 2000 e o teste
-- passa de forma trivial.
-- Gatilho de ação: localizar o `turma_id` afetado; checar com a
-- origem se é (a) reuso de id entre anos, caso em que o grão upstream
-- precisa virar `(turma_id, ano_letivo)` ou o desempate precisa usar
-- `aluno_id` e ano, ou (b) bug de ingestão de `turma`. Fora de
-- `prod_bq`, severidade `warn` (a amostra aleatória pode gerar colisão
-- sintética); em `prod_bq`, `error` é o gate.
{{
    config(
        severity="error" if target.name == "prod_bq" else "warn"
    )
}}

select
    turma_id,
    count(distinct ano_letivo) as n_anos_distintos
from {{ ref('stg_educacao__turma') }}
where
    turma_id is not null
    and ano_letivo is not null
group by turma_id
having count(distinct ano_letivo) > 1
