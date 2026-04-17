# Agent Prompt — `reitbrazil-sync`

> Este documento é o briefing completo para o agente que vai construir o
> projeto `reitbrazil-sync` do zero. Leia do início ao fim antes de começar
> a executar. Ao final, seu trabalho é ter um pipeline funcionando e
> testado. Escreva código, commits e documentação em português/inglês
> conforme indicado em cada seção.

---

## 1. Missão

Construir um CLI em Go chamado **`reitbrazilctl`** que:

1. Baixa dados reais de Fundos Imobiliários brasileiros (FIIs) de fontes
   estáveis e legais.
2. Armazena os dados **crus** em **BigQuery** (camada bronze,
   append-only, auditável).
3. Transforma em tabelas **canônicas** em BigQuery (camada silver,
   deduplicadas, idempotentes).
4. Exporta um arquivo **SQLite** (`reitbrazil.db`) no formato exato
   consumido pelo servidor MCP irmão em
   `/Users/addo/jobs/addodelgrossi/reitbrazil/`.
5. Publica o SQLite em **GCS** (`gs://reitbrazil-db/latest/`) todos os
   dias úteis e cria um **GitHub Release** mensal como snapshot
   auditável.

O servidor MCP `reitbrazil` é **somente consumidor** do `reitbrazil.db`.
Este projeto (`reitbrazil-sync`) é o produtor. O contrato entre os dois
é **exclusivamente o schema do SQLite**.

---

## 2. Preflight (leia antes de escrever qualquer código)

Antes de qualquer ação, **leia** os seguintes arquivos. Eles são
autoridade sobre o schema e sobre convenções que você deve respeitar:

1. `/Users/addo/jobs/addodelgrossi/reitbrazil/internal/storage/migrations/0001_init.sql`
2. `/Users/addo/jobs/addodelgrossi/reitbrazil/internal/storage/migrations/0002_indexes.sql`
3. `/Users/addo/jobs/addodelgrossi/reitbrazil/internal/storage/migrations/0003_views.sql`
4. `/Users/addo/jobs/addodelgrossi/reitbrazil/internal/domain/*.go` (entidades que o MCP espera encontrar na base)
5. `/Users/addo/jobs/addodelgrossi/reitbrazil/internal/sqlite/*.go` (queries que o MCP vai executar contra o banco que você gerar)
6. `/Users/addo/.claude/plans/quero-que-voc-atue-eventual-clover.md` (plano de arquitetura deste projeto)

Depois, confirme com `git status` / `ls` o estado atual do repo em
`/Users/addo/jobs/addodelgrossi/reitbrazil-sync/`. O repo começa
praticamente vazio (`.gitignore`, `README.md`).

Se algo neste prompt conflitar com o que você observa nos arquivos do
MCP, **a realidade do MCP ganha** — ajuste e sinalize a divergência.

---

## 3. Parâmetros fixos

| Parâmetro | Valor |
|---|---|
| Diretório do projeto | `/Users/addo/jobs/addodelgrossi/reitbrazil-sync/` |
| Remote git | `git@github.com:addodelgrossi/reitbrazil-sync.git` |
| Module path | `github.com/addodelgrossi/reitbrazil-sync` |
| Binário principal | `reitbrazilctl` (em `cmd/reitbrazilctl/`) |
| Projeto GCP | `reitbrazil` |
| Dataset BQ raw | `reitbrazil_raw` |
| Dataset BQ canon | `reitbrazil_canon` |
| Location BQ/GCS | `southamerica-east1` |
| Bucket GCS | `reitbrazil-db` |
| Repo do MCP (consumidor) | `github.com/addodelgrossi/reitbrazil` |
| Go | 1.26 (o mais novo disponível; use features novas) |
| Licença | MIT (copie o header do repo MCP irmão) |

Nada acima é negociável sem permissão explícita do usuário.

---

## 4. Escopo (Option C = v1 completa com deploy)

Entregue tudo abaixo nesta ordem. Cada bloco deve virar pelo menos um
commit legível (prefira PRs pequenos se for um fluxo colaborativo):

### 4.1 Bootstrap
- `go.mod` no module path acima
- `Makefile` com alvos: `build`, `run-daily`, `run-monthly`, `test`,
  `lint`, `tidy`, `clean`, `docker-build`, `docker-push`, `tf-plan`,
  `tf-apply`
- `.golangci.yml` (use o mesmo estilo do repo MCP irmão)
- `.env.example` listando TODAS as variáveis (valores fake)
- `.gitignore` estendido (`.env`, `bin/`, `out/`, `*.db`, `dist/`)
- `LICENSE` (MIT), `README.md` (seção "Quick start" + "Architecture" +
  links para docs)
- `docs/architecture.md`, `docs/sources.md`, `docs/bigquery.md`,
  `docs/deploy.md`

### 4.2 Domain layer
- `internal/model/` — entidades internas do pipeline. **Não reutilize**
  structs do repo MCP. Modelos aqui representam eventos de ingestão,
  não entidades de domínio do serviço.
  - `event.go` com `RawEvent { Source, Kind, Payload []byte, IngestedAt }`
  - `fund.go`, `price.go`, `dividend.go`, `fundamentals.go`
- Tipos VO para `Ticker` (regex `[A-Z]{4}\d{2}`), consistente com o
  que o MCP valida.

### 4.3 Config & logging
- `internal/config/config.go` — carrega `.env` (use
  `github.com/joho/godotenv` ou parser próprio), aplica env vars,
  opcionalmente lê `config.yaml` (sobrepõe .env). Prefixo `INGEST_`.
- `internal/logging/logging.go` — `slog.New(slog.NewJSONHandler(...))`,
  nível configurável, atributos padrão `service=reitbrazil-sync`,
  `run_id=<uuid>`.

### 4.4 Source adapters
Diretório: `internal/sources/`.

#### 4.4.1 `brapi/`
- URL base: `https://brapi.dev/api`
- Auth: Bearer token (`Authorization: Bearer <token>`) do env
  `INGEST_BRAPI_TOKEN`.
- `client.go`: `*http.Client` com:
  - `golang.org/x/time/rate.Limiter` (rps do `INGEST_RATE_LIMIT_RPS`,
    default 3)
  - retry exponencial próprio em 429 e 5xx (3 tentativas, base 500ms)
  - User-Agent `reitbrazil-sync/<version>`
  - context propagation em toda chamada
- `funds.go`: `FetchList(ctx) iter.Seq2[Fund, error]` via
  `/quote/list?type=fund&limit=100&page=N` até `hasNextPage=false`.
- `prices.go`: `FetchHistory(ctx, ticker, from, to) iter.Seq2[PriceBar, error]`
  via `/quote/{ticker}?range=5y&interval=1d` (ou `startDate`/`endDate`).
- `dividends.go`: `Fetch(ctx, ticker) iter.Seq2[Dividend, error]` via
  `/quote/{ticker}?dividends=true`. Retorna lista ordenada por `ex_date
  DESC` já mapeada.
- `fundamentals.go`: `Fetch(ctx, ticker) (Fundamentals, error)` via
  `/quote/{ticker}?modules=defaultKeyStatistics,financialData` (plano
  Pro).
- `types.go`: DTOs da API (structs que casam com a resposta JSON do
  brapi; **não vaze** esses tipos para fora do pacote).

Fixtures para teste: salve em `testdata/fixtures/brapi/*.json`
amostras reais (sanitizadas se precisar) e escreva unit tests contra
elas.

#### 4.4.2 `cvm/`
- `downloader.go`: baixa ZIPs de
  `https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/`
- `parser.go`: abre ZIP, parseia CSV estruturado. Retorna
  `iter.Seq2[InformeMensal, error]`.
- Cadência: apenas mensal (CVM atualiza 1×/mês).
- `types.go`: mapeamento completo dos campos relevantes
  (CNPJ, número de cotistas, PL, NAV por cota, vacâncias).

#### 4.4.3 `b3/` (opcional v1, pode ficar para v1.1)
- Parser do COTAHIST se houver tempo. Caso contrário, crie o diretório
  com `doc.go` e um `// TODO(v1.1)` marcando o layout de 245 bytes.

### 4.5 BigQuery
Diretório: `internal/bq/`.

- `client.go`: cria `*bigquery.Client`, aplica location `southamerica-east1`,
  bootstrap dos datasets `reitbrazil_raw` e `reitbrazil_canon` se não
  existirem (idempotente, pode rodar em todo startup).
- `schema.go`: define schemas das tabelas raw (bronze) e canon (silver).
- `land.go`:
  - Escolha streaming inserts para volumes pequenos (<10k rows/batch) ou
    load jobs com arquivos NDJSON temporários para grandes (history
    completo).
  - Writer genérico `Land[T any](ctx, table, rows iter.Seq2[T, error])`.
- `transform.go`: executa arquivos `.sql` do diretório `bq_sql/` em
  ordem.
- `read.go`: `Read[T any](ctx, query string) iter.Seq2[T, error]` —
  paginação automática, usa BQ iterator do SDK.

Tabelas raw (append-only, particionadas por `DATE(ingested_at)`):
- `raw.brapi_fund_list(ticker STRING, short_name STRING, long_name STRING, payload JSON, ingested_at TIMESTAMP)`
- `raw.brapi_quote(ticker STRING, trade_date DATE, open FLOAT64, high FLOAT64, low FLOAT64, close FLOAT64, volume INT64, payload JSON, ingested_at TIMESTAMP)`
- `raw.brapi_dividends(ticker STRING, ex_date DATE, payment_date DATE, amount FLOAT64, kind STRING, payload JSON, ingested_at TIMESTAMP)`
- `raw.brapi_fundamentals(ticker STRING, as_of DATE, payload JSON, ingested_at TIMESTAMP)`
- `raw.cvm_informe_mensal(cnpj STRING, ticker STRING, reference_month DATE, num_investors INT64, equity_total FLOAT64, nav_per_share FLOAT64, vacancy_physical FLOAT64, vacancy_financial FLOAT64, payload JSON, ingested_at TIMESTAMP)`

Tabelas canon (silver, deduplicadas):
- `canon.funds` — unique `ticker`. Cluster by `(segment, mandate)`.
- `canon.prices` — unique `(ticker, trade_date)`. Partition by
  `trade_date`, cluster by `ticker`.
- `canon.dividends` — unique `(ticker, ex_date, kind)`.
- `canon.fundamentals` — unique `(ticker, as_of)`.
- `canon.fund_snapshots` — rollup materializado
  (dy_trailing_12m, dy_forward_est, avg_daily_volume_90d,
  volatility_90d, max_drawdown_1y, pvp).

### 4.6 SQL do BigQuery
Diretório: `bq_sql/` (embedded via `//go:embed`).

- `01_create_raw_tables.sql`
- `02_create_canon_tables.sql`
- `10_transform_funds.sql`
- `11_transform_prices.sql`
- `12_transform_dividends.sql`
- `13_transform_fundamentals.sql`
- `20_materialize_snapshots.sql`

Todos os transforms devem ser **`MERGE` statements idempotentes** — rodar
N vezes deve dar o mesmo resultado. Sempre selecione apenas o
`ingested_at` mais recente por PK lógica (use `QUALIFY
ROW_NUMBER() OVER ...` ou `MAX(ingested_at) OVER ...`).

Exemplo (`11_transform_prices.sql`):
```sql
MERGE `${project}.reitbrazil_canon.prices` T
USING (
  SELECT ticker, trade_date, open, high, low, close, volume
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY ticker, trade_date ORDER BY ingested_at DESC) rn
    FROM `${project}.reitbrazil_raw.brapi_quote`
  )
  WHERE rn = 1
) S
ON T.ticker = S.ticker AND T.trade_date = S.trade_date
WHEN MATCHED AND (T.close != S.close OR T.volume != S.volume)
  THEN UPDATE SET open=S.open, high=S.high, low=S.low, close=S.close, volume=S.volume
WHEN NOT MATCHED
  THEN INSERT (ticker, trade_date, open, high, low, close, volume)
       VALUES (S.ticker, S.trade_date, S.open, S.high, S.low, S.close, S.volume);
```

Faça substituição de `${project}` em tempo de execução.

### 4.7 Export para SQLite
Diretório: `internal/export/`.

- `migrations_sqlite/` — **cópia literal** das migrations do repo MCP
  (`0001_init.sql`, `0002_indexes.sql`, `0003_views.sql`). Embed via
  `//go:embed`.
- `sqlite.go` — abre arquivo SQLite novo (truncating se existir),
  aplica migrations, lê de BQ (`bq.Read[T]`) e faz INSERT em batch
  (transações de 1000 rows).
- **Driver**: use `modernc.org/sqlite` (mesmo do MCP — sem CGO).
- O arquivo final deve passar no teste do MCP: abrir em
  `:memory:`/file e executar queries simples deve funcionar sem erros.

### 4.8 Publish
Diretório: `internal/publish/`.

- `gcs.go`:
  - Upload `reitbrazil.db` para `gs://reitbrazil-db/latest/reitbrazil.db`
  - Upload cópia versionada para
    `gs://reitbrazil-db/history/reitbrazil-YYYY-MM-DD.db`
  - Upload sidecar `latest/metadata.json` com
    `{version, generated_at, fund_count, price_rows, dividend_rows,
    fundamentals_rows, git_sha}`
- `github.go` (apenas para `run monthly`):
  - Cria release `data-v<YYYY>.<MM>` via API (`google/go-github/v66`)
  - Sobe `reitbrazil.db` + `metadata.json` como assets
  - Body com diff de rows vs release anterior

### 4.9 Pipeline
Diretório: `internal/pipeline/`.

- `orchestrator.go` — compõe estágios: `fetch → land → transform →
  export → publish`. Cada estágio recebe `ctx` e retorna
  `StageResult{Stage, RowsProcessed, DurationMs, Errors []error}`.
- `retry.go` — helpers de retry (talvez só exponential backoff).
- Use `errors.Join` para agregar falhas parciais por ticker sem abortar
  o pipeline.
- Gere `run_id` UUID no início e anexe a todos os logs/metadados.

### 4.10 CLI
Diretório: `internal/cli/` + `cmd/reitbrazilctl/main.go`.

Framework: `spf13/cobra` + `charmbracelet/fang` (wraps cobra com
styling). Todos os comandos:
- Aceitam context (`signal.NotifyContext(SIGINT,SIGTERM)`)
- Emitem slog JSON em stderr
- Retornam exit code 0 em sucesso, 1 em erro

Comandos:
```
reitbrazilctl doctor
reitbrazilctl discover [--dry-run]
reitbrazilctl fetch prices       [--tickers X,Y] [--from YYYY-MM-DD] [--to YYYY-MM-DD]
reitbrazilctl fetch dividends    [--tickers X,Y]
reitbrazilctl fetch fundamentals [--tickers X,Y]
reitbrazilctl fetch cvm          [--month YYYY-MM]
reitbrazilctl transform          [--stage funds|prices|dividends|fundamentals|snapshots|all]
reitbrazilctl export sqlite      --output ./out/reitbrazil.db
reitbrazilctl publish gcs        --input ./out/reitbrazil.db
reitbrazilctl publish release    --input ./out/reitbrazil.db
reitbrazilctl run daily
reitbrazilctl run monthly
```

Flags globais:
- `--log-level`, `--log-format`, `--config` (caminho do config.yaml)
- `--project-id` (default do env)
- `--dry-run` (não escreve em BQ/GCS, só loga o que faria)

`doctor` valida:
- `.env` carregado e variáveis obrigatórias presentes
- Ping brapi (`/quote/list?limit=1`)
- Autenticação GCP (lista datasets do projeto)
- Bucket GCS acessível
- Aplica migrations do SQLite em `:memory:` (smoke)

### 4.11 Configuração (`.env`)
Crie `.env.example` com todas as variáveis. Leitura via
`github.com/joho/godotenv` com precedência: env real > arquivo `.env`.

```env
# Required
INGEST_BRAPI_TOKEN=...
INGEST_GCP_PROJECT=reitbrazil
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# BigQuery
INGEST_BQ_DATASET_RAW=reitbrazil_raw
INGEST_BQ_DATASET_CANON=reitbrazil_canon
INGEST_BQ_LOCATION=southamerica-east1

# GCS
INGEST_GCS_BUCKET=reitbrazil-db
INGEST_GCS_KEY_LATEST=latest/reitbrazil.db

# GitHub (only for monthly release)
INGEST_GITHUB_TOKEN=...
INGEST_GITHUB_REPO=addodelgrossi/reitbrazil

# Behaviour
INGEST_LOG_LEVEL=info
INGEST_LOG_FORMAT=json
INGEST_RATE_LIMIT_RPS=3
```

O `.env` **nunca** deve ser commitado. `.env.example` deve ter placeholders
óbvios.

### 4.12 Deploy (v1 completa — option C)
Diretório: `deploy/`.

#### 4.12.1 `Dockerfile`
- Multi-stage: `golang:1.26-alpine` para build, `gcr.io/distroless/static`
  para runtime.
- Binário estático (`CGO_ENABLED=0`).
- `ENTRYPOINT ["/reitbrazilctl"]`.

#### 4.12.2 `cloudbuild.yaml`
- Build da imagem
- Push para Artifact Registry
  (`southamerica-east1-docker.pkg.dev/reitbrazil/reitbrazilctl`)
- Tag com git SHA + `latest`

#### 4.12.3 `deploy/terraform/`
Provisiona tudo:
- `provider.tf` — `google` e `google-beta` apontando para projeto
  `reitbrazil`
- `bq.tf` — datasets `reitbrazil_raw` e `reitbrazil_canon`,
  location `southamerica-east1`
- `gcs.tf` — bucket `reitbrazil-db` com object versioning,
  lifecycle (expire `history/` após 365d)
- `artifact_registry.tf` — repo Docker
- `secret_manager.tf` — secrets `brapi-token` e `github-token`
- `iam.tf` — service account `reitbrazilctl-runner` com papéis:
  - `roles/bigquery.dataEditor` (raw), `roles/bigquery.jobUser`
  - `roles/storage.objectAdmin` no bucket `reitbrazil-db`
  - `roles/secretmanager.secretAccessor`
- `cloud_run_jobs.tf` — dois jobs:
  - `reitbrazil-sync-daily` com comando
    `["run","daily"]`
  - `reitbrazil-sync-monthly` com comando
    `["run","monthly"]`
  - Cada um com as secret refs injetadas como env
- `cloud_scheduler.tf` — dois triggers:
  - diário: cron `15 22 * * 1-5` timezone `America/Sao_Paulo`
  - mensal: cron `30 23 2 * *` timezone `America/Sao_Paulo`
- `outputs.tf` — URL dos jobs, nome do bucket, emails das SAs

README em `deploy/terraform/README.md` com o passo-a-passo:
1. `terraform init`
2. `terraform apply -var="project_id=reitbrazil"`
3. Popular secrets manualmente:
   `gcloud secrets versions add brapi-token --data-file=-`
4. Test run: `gcloud run jobs execute reitbrazil-sync-daily`

### 4.13 Testing
Três tiers:

**Unit** (rápido, sem rede):
- `internal/model/*_test.go`
- `internal/sources/brapi/*_test.go` com fixtures
- `internal/sources/cvm/*_test.go` com fixtures
- `internal/export/*_test.go` — testa que SQLite gerado tem todas as
  tabelas do MCP

**Integration** (tag `//go:build integration`):
- `internal/bq/*_test.go` — usa dataset real temporário
  (`reitbrazil_it_<random>`), cleanup ao final
- `internal/publish/gcs_test.go` — mesmo padrão

**Contract test** (o mais importante):
- `internal/export/contract_test.go` — cria SQLite mínimo via
  `exec sqlite`, abre com as queries EXATAS que
  `/Users/addo/jobs/addodelgrossi/reitbrazil/internal/sqlite/*.go`
  executa (importe e reuse essas queries se viável, ou reescreva
  equivalentes). Se alguma query falha, o contract está quebrado e
  build falha.

CI: GitHub Actions rodando `go vet`, `go test -race ./...`,
`golangci-lint`. Integration tests num workflow separado gated por
label (`run-integration`).

---

## 5. Invariantes obrigatórias

Viole-as e o pipeline falha:

1. **Schema do SQLite idêntico ao do MCP.** Copie as migrations. Se
   precisar mudar, crie uma migration nova TANTO aqui quanto no MCP e
   sinalize ao usuário.
2. **Transforms idempotentes.** `run daily` 3× seguidas deve produzir
   resultado idêntico à 1ª.
3. **Raw é imutável.** Nunca `UPDATE`/`DELETE` em `raw.*`. Apenas
   `INSERT` com `ingested_at`.
4. **Secrets fora do git.** `.env` no `.gitignore`. Nada de token
   commitado.
5. **Determinismo no export.** Ordens `ORDER BY ticker ASC,
   trade_date ASC` etc para que dois runs do mesmo raw gerem SQLites
   byte-diferentes só pelos campos de `updated_at`, não pelas linhas.
6. **Quality gate antes de publicar.** Se `fund_count < 100` ou
   `MAX(trade_date) < today-3` no canon, aborte publish e escreva
   `runs/error-YYYY-MM-DD.json` no bucket.

---

## 6. Verificação end-to-end (aceite)

Só considere v1 pronta quando TUDO abaixo funcionar:

1. `make test` — unit + contract passam.
2. `make lint` — limpo.
3. Localmente, com `.env` preenchido:
   ```
   reitbrazilctl doctor                   # tudo OK
   reitbrazilctl fetch prices --tickers XPLG11,HGLG11 --from 2026-01-01
   reitbrazilctl transform --stage prices
   reitbrazilctl export sqlite --output ./out/reitbrazil.db
   ```
4. Rode o MCP server irmão contra o SQLite que você gerou:
   ```
   REITBR_DB_PATH=$(pwd)/out/reitbrazil.db \
     /Users/addo/jobs/addodelgrossi/reitbrazil/bin/reitbrazil
   ```
   Em outro terminal, dispare manualmente (via cliente MCP ou via pipe
   JSON-RPC) e confirme:
   - `health_check` → status=ok, fund_count>=N
   - `list_funds limit=5` retorna FIIs reais (não os sintéticos do seed)
   - `get_dividend_history ticker=XPLG11` retorna eventos de 2025-2026
5. `cd deploy/terraform && terraform plan` — sem erros.
6. `terraform apply` bem-sucedido num projeto GCP de teste OU script
   `deploy/dry-run.sh` mostra o que seria criado.
7. `gcloud run jobs execute reitbrazil-sync-daily --region=southamerica-east1`
   executa sem erro em um projeto de teste e cria
   `gs://reitbrazil-db/latest/reitbrazil.db`.

---

## 7. Out of scope

Não faça (fica para v1.1+ explicitamente):
- Parser do COTAHIST da B3 (crie só o esqueleto com TODO).
- Cobertura de FI-Infra, CRI, LCI.
- Tool `refresh_data` no MCP para reload sem restart.
- Dashboard Looker Studio.
- Alertas Cloud Monitoring (mencione em `docs/deploy.md` como próximo passo).
- Testes de caos / injeção de falha em BQ.
- Suporte a Postgres como destino.

---

## 8. Como trabalhar

- **Ordem dos commits** (cada um deve deixar o projeto em estado
  build-ável):
  1. Bootstrap (`go.mod`, Makefile, LICENSE, README, `.env.example`,
     `.golangci.yml`)
  2. Model + config + logging
  3. brapi client (+ testes de fixture)
  4. cvm adapter
  5. BigQuery client + schema + land
  6. SQL transforms em `bq_sql/`
  7. Export para SQLite (+ contract test)
  8. Publish (GCS + GitHub)
  9. Pipeline orchestrator
  10. CLI (cobra + fang)
  11. Dockerfile + cloudbuild
  12. Terraform
  13. Docs (`docs/*.md`) + README final

- **Quando pedir confirmação ao usuário**:
  - Você encontrou um conflito entre este prompt e o código do MCP
    (escolha do schema). Pare, descreva a divergência, proponha um
    caminho e pergunte.
  - A API da brapi retorna algo diferente do documentado e isso muda
    a semântica (ex.: dividend kind ausente). Pare, mostre o payload,
    proponha.
  - Terraform vai criar algo que custa dinheiro recorrente e não está
    no plano (default: o pipeline cabe no free tier GCP).

- **Quando apenas decidir e seguir**:
  - Nomes de variáveis internas, ordem de testes, detalhes de layout
    de código.
  - Versões exatas de dependências (use as mais recentes estáveis).
  - Mensagens de log.
  - Ajustes de `.golangci.yml` para tipos de warning benignos.

- **Ao terminar**, entregue um `SUMMARY.md` curto listando:
  - O que foi feito (bullets por commit)
  - O que ficou pendente e por quê
  - Como rodar localmente (passo a passo)
  - Como fazer o primeiro deploy

---

## 9. Dicas técnicas finais

- **Go 1.26 features a usar**: `iter.Seq2[K,V]` em repositórios,
  `testing.T.Context()` em todo teste novo, `errors.Join` para erros
  parciais, generics em helpers BQ, `sync.OnceValue` para clients
  preguiçosos, `slices.*` e `maps.*` em vez de loops manuais.
- **Observabilidade**: gere `run_id` UUID uma vez no início de
  `run daily/monthly` e propague via context value + log attr. Isso faz
  os logs correlacionáveis no Cloud Logging.
- **Rate limiting**: pense conservador contra a brapi. Plano Pro tem
  500K req/mês; não queime em uma rodada buggy. 3 rps = 258K/dia,
  sobra.
- **Custos**: este pipeline cabe no free tier GCP (BQ 1 TB
  query/mês, GCS 5 GB, Cloud Run Jobs tier gratuito limitado mas
  suficiente aqui). Se Terraform indicar algo pago não-óbvio,
  sinalize antes de aplicar.
- **Don't over-engineer**: não crie abstrações para "caso haja outra
  fonte". Há duas fontes hoje (brapi, CVM). Se chegar uma terceira,
  extrai interface ali. YAGNI.

---

## 10. Quando este prompt estiver desatualizado

Se você leu isto e algo não bate com a realidade (schema do MCP mudou,
API da brapi mudou, usuário pediu outra coisa no meio do caminho),
**atualize este arquivo** com um patch no final (seção `## Changelog`)
e siga.

Boa jornada. O usuário está disponível para dúvidas bloqueantes.
Evite perguntas de estilo; vá com defaults sensatos.
