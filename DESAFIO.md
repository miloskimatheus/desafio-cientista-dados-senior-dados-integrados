# Desafio Técnico - Cientista de Dados Sênior

## Registro Municipal Integrado (RMI)

---

## Contexto

O **Registro Municipal Integrado (RMI)** é o data warehouse estratégico da Prefeitura do Rio de Janeiro, que consolida dados de saúde, educação, assistência social e dezenas de outros sistemas municipais. A qualidade, confiabilidade e rastreabilidade desses dados são fundamentais para políticas públicas baseadas em evidências.

Como Cientista de Dados Sênior no RMI, você liderará a modelagem analítica e a governança de dados que orientam decisões para milhões de cariocas. A **engenharia de analytics** — transformação, teste e documentação de dados — é a espinha dorsal da função.

Este desafio avalia suas habilidades em **modelagem de dados com dbt**, estratégia de testes e qualidade, SQL analítico e documentação técnica, usando dados educacionais anonimizados que simulam o contexto real do RMI.

---

## Instruções

1. Faça um fork do repositório do desafio para colocar a sua solução
2. Organize o projeto seguindo as boas práticas de dbt descritas abaixo
3. Inclua **README.md** explicando abordagem, decisões de arquitetura e como reproduzir
4. **Entrega**: Envie o link do repositório para `selecao.pcrj@gmail.com`

---

## Dados

Os dados deste desafio representam registros educacionais da rede municipal, **anonimizados e aleatorizados** para fins de avaliação. Eles foram disponibilizados como arquivos Parquet em um bucket do Google Cloud Storage (GCS).

### Tabelas Disponíveis

| Arquivo (GCS) | Descrição | Granularidade |
|----------------|-----------|---------------|
| `aluno.parquet` | Cadastro de alunos (IDs anonimizados, faixa etária, bairro anonimizado) | 1 linha por aluno |
| `escola.parquet` | Cadastro de unidades escolares (IDs anonimizados, tipo, região) | 1 linha por escola |
| `turma.parquet` | Informações de turmas (série, turno, ano letivo) | 1 linha por turma |
| `frequencia.parquet` | Registros de frequência escolar | 1 linha por aluno/dia |
| `avaliacao.parquet` | Notas e avaliações por bimestre | 1 linha por aluno/disciplina/bimestre |

### Tabela Auxiliar Pública

| Tabela (BigQuery) | Descrição |
|--------------------|-----------|
| `datario.dados_mestres.bairro` | Cadastro de bairros do Rio de Janeiro |

### Acesso aos dados

Os arquivos Parquet estão disponíveis no bucket GCS:

```
https://console.cloud.google.com/storage/browser/case_vagas/rmi
```

Alternativamente, os arquivos podem ser carregados diretamente em um dataset BigQuery pessoal para uso com dbt-bigquery (veja instruções no FAQ).

**⚠️ IMPORTANTE sobre os dados:**

- Os dados foram **anonimizados**: todos os identificadores pessoais (nomes, CPFs, endereços) foram removidos e substituídos por IDs sintéticos
- Os valores numéricos foram **aleatorizados** (com preservação de distribuições e relações entre tabelas)
- **Não tente reidentificar** registros ou cruzar com fontes externas
- Os dados servem para avaliar sua capacidade técnica de modelagem, não para gerar insights reais sobre a rede educacional

---

## Parte 1: Projeto dbt e Modelagem

**Construa um projeto dbt do zero** (`dbt-core` + adapter de sua escolha) que transforme os dados brutos em camadas analíticas.

### 1. Configuração e Estrutura do Projeto

Configure o projeto dbt com sources apontando para os dados disponibilizados. Organize os models seguindo a convenção de camadas staging → intermediate → marts.

**Entregue**: Projeto dbt funcional (`dbt run` executa sem erros), com `dbt_project.yml` configurado, sources definidas e estrutura de diretórios organizada.

### 2. Camada de Staging

Crie models de staging para **pelo menos 4 tabelas** fonte. Aplique limpeza, padronização de nomes, tipagem, tratamento de nulos e filtros básicos de qualidade.

**Entregue**: Models de staging com naming convention consistente, lógica de limpeza documentada e `schema.yml` com descrições das sources e colunas.

### 3. Camada Intermediate

Crie **pelo menos 1 model intermediate** que combine dados de múltiplas fontes. Por exemplo: um modelo que una alunos + matrículas + frequência para calcular métricas de presença por aluno/período.

**Entregue**: Model(s) intermediate com lógica de join documentada (premissas, tipo de join, tratamento de registros órfãos) e justificativa das decisões de modelagem.

### 4. Camada de Marts

Construa **pelo menos 1 mart** orientado a responder uma pergunta analítica relevante para gestores públicos. Sugestões (ou proponha a sua):

- **Absenteísmo crônico por região:** Quais escolas/regiões têm maior taxa de alunos com frequência abaixo de 75%?
- **Desempenho por perfil de turma:** Como a composição das turmas (tamanho, série, turno) se correlaciona com desempenho nas avaliações?

**Entregue**: Mart(s) materializados como table ou incremental, com `schema.yml` contendo descrições, e uma breve análise dos resultados (pode ser no README ou em um notebook auxiliar).

---

## Parte 2: Testes e Qualidade de Dados

**Esta é a parte mais importante do desafio.** A confiabilidade dos dados do RMI depende de uma estratégia sólida de testes.

### 5. Testes Genéricos

Aplique testes built-in do dbt (`unique`, `not_null`, `accepted_values`, `relationships`) nos models de staging e marts. A cobertura deve ser intencional — não basta testar tudo mecanicamente, queremos ver critério na escolha do que testar e por quê.

**Entregue**: Testes configurados nos `schema.yml`, todos passando com `dbt test`. Documente brevemente por que cada teste é relevante (pode ser em comentários no YAML ou no README).

### 6. Testes de Regra de Negócio

Crie **pelo menos 2 testes singular ou customizados** que validem regras de negócio educacionais. Exemplos:

- "Nenhum registro de frequência deve ter data anterior à data de matrícula do aluno naquela turma"
- "Todo aluno com movimentação de tipo 'abandono' deve ter sua última frequência registrada antes da data da movimentação"
- "A soma de presença + ausência por aluno/dia não deve ultrapassar a carga horária da turma"

**Entregue**: Testes implementados (em `tests/` ou como macros), passando com `dbt test`, com documentação explicando a regra validada e por que ela importa.

---

## Parte 3: Documentação e Análise

### 8. Documentação do Projeto

Produza documentação que permita a um novo membro do time entender e dar manutenção ao projeto.

**Entregue**: README.md com instruções de setup e execução, diagrama ou descrição do lineage (staging → intermediate → marts), decisões de arquitetura (materializations, naming conventions, estratégia de testes), trade-offs identificados e o que faria diferente com mais tempo.

### 9. Análise Exploratória (opcional, diferencial)

Elabore um notebook Python complementar com uma análise exploratória dos dados que justifique suas escolhas de modelagem.

**Entregue**: Notebook documentado mostrando padrões encontrados nos dados, anomalias ou problemas de qualidade identificados, e como isso influenciou suas decisões no dbt.

---

## Avaliação

Você será avaliado em cada uma das categorias abaixo:

- **Modelagem e arquitetura dbt**
- **Estratégia de testes e qualidade**
- **SQL e lógica analítica**
- **Documentação e comunicação**

Os melhores candidatos serão chamados para a etapa de entrevistas.

**Dica**: profundidade importa mais que completude. Melhor ter menos models com testes excelentes e documentação clara do que muitos models superficiais.

### Diferenciais

- Uso de dbt packages (`dbt_utils`, `dbt_expectations`, `codegen`)
- Configuração de `freshness` nas sources
- Uso de tags, hooks ou exposures
- Testes de data contracts
- Análise exploratória complementar (Parte 3, questão 9)
- Configuração de CI com `dbt build` em GitHub Actions

---

## Estrutura Sugerida do Repositório

```
desafio-rmi-ds/
├── README.md
├── dbt_project.yml
├── packages.yml              # (se usar dbt packages)
├── models/
│   ├── staging/
│   │   ├── stg_educacao__aluno.sql
│   │   ├── stg_educacao__escola.sql
│   │   ├── ...
│   │   └── schema.yml
│   ├── intermediate/
│   │   ├── int_educacao__aluno_frequencia.sql
│   │   └── schema.yml
│   └── marts/
│       ├── mart_educacao__absenteismo.sql
│       └── schema.yml
├── tests/                    # Testes singular
├── macros/                   # Macros customizadas
├── seeds/                    # Dados auxiliares (se aplicável)
├── notebooks/                # (opcional) Análise exploratória
│   └── eda.ipynb
├── data/                     # Parquets baixados (não commitar)
│   └── .gitkeep
└── requirements.txt
```

---

## FAQ

**1. Preciso usar BigQuery como warehouse?**
Sim, BigQuery é o warehouse do projeto. Os Parquets são carregados via `bq load` para dataset raw do BigQuery Sandbox. O importante é que o projeto dbt funcione end-to-end.

**4. Os dados estão anonimizados — posso confiar nas relações entre tabelas?**
Sim. As chaves de relacionamento (IDs de aluno, escola, turma) foram preservadas consistentemente entre as tabelas. As distribuições estatísticas foram mantidas, mas os valores individuais foram aleatorizados.

**5. Preciso fazer todas as partes?**
Sim, exceto a Parte 3 questão 9 (análise exploratória), que é opcional. Mas profundidade importa mais que completude — melhor fazer menos com excelência.

**6. Posso usar dbt packages?**
Sim, e é um diferencial! Sugestões: `dbt_utils`, `dbt_expectations`, `codegen`.

---

## Contato

Dúvidas? Envie um email para: **<fernanda.scovino@prefeitura.rio>**

Boa sorte! 🚀
