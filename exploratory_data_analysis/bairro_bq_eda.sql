-- eda de datario.dados_mestres.bairro
-- executado em 2026-04-23 - projeto <DBT_GCP_PROJECT> - região US
-- objetivo: confirmar schema, volumetria, completude e incompatibilidade de fk
-- com os campos bairro (hash) dos parquets anonimizados
-- resultados consolidados em REPORT.md §5 (achado 5)

-- pega o schema via information_schema
select
    column_name,
    data_type,
    is_nullable,
    ordinal_position
from `datario.dados_mestres.INFORMATION_SCHEMA.COLUMNS`
where table_name = 'bairro'
order by ordinal_position;

-- pega amostra (sem colunas geométricas)
select
    id_bairro,
    nome,
    id_area_planejamento,
    id_regiao_planejamento,
    nome_regiao_planejamento,
    id_regiao_administrativa,
    nome_regiao_administrativa,
    subprefeitura,
    area,
    perimetro
from `datario.dados_mestres.bairro`
order by safe_cast(id_bairro as int64)
tablesample system (10 percent);

-- calcula volumetria, cardinalidade e nulls por coluna
select
    count(*) as total_rows,
    count(distinct id_bairro) as distinct_id_bairro,
    count(distinct nome) as distinct_nome,
    count(distinct id_area_planejamento) as distinct_ap,
    count(distinct id_regiao_planejamento) as distinct_rp,
    count(distinct id_regiao_administrativa) as distinct_ra,
    count(distinct subprefeitura) as distinct_subprefeitura,
    countif(id_bairro is null) as null_id_bairro,
    countif(nome is null) as null_nome,
    countif(id_area_planejamento is null) as null_ap,
    countif(id_regiao_planejamento is null) as null_rp,
    countif(id_regiao_administrativa is null) as null_ra,
    countif(subprefeitura is null) as null_subprefeitura,
    countif(area is null) as null_area,
    countif(perimetro is null) as null_perimetro,
    countif(geometry_wkt is null) as null_geom_wkt,
    countif(geometry is null) as null_geometry
from `datario.dados_mestres.bairro`;

-- range e castabilidade de id_bairro para int64
-- (determina se o campo pode ser joinado contra cast(hash as int64))
select
    min(safe_cast(id_bairro as int64)) as id_min,
    max(safe_cast(id_bairro as int64)) as id_max,
    countif(safe_cast(id_bairro as int64) is null) as nao_numerico,
    min(area) as area_min_m2,
    max(area) as area_max_m2,
    min(perimetro) as perimetro_min_m,
    max(perimetro) as perimetro_max_m
from `datario.dados_mestres.bairro`;

-- distribuição por área de planejamento
select
    id_area_planejamento,
    count(*) as n_bairros
from `datario.dados_mestres.bairro`
group by id_area_planejamento
order by id_area_planejamento;

-- distribuição por região administrativa
select
    nome_regiao_administrativa,
    count(*) as n_bairros
from `datario.dados_mestres.bairro`
group by nome_regiao_administrativa
order by n_bairros desc;
