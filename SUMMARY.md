# reitbrazil-sync — v1 Summary

Data pipeline that produces the `reitbrazil.db` SQLite file consumed by
the sibling [`reitbrazil`](https://github.com/addodelgrossi/reitbrazil)
MCP server. Contract between the two repos is the SQLite schema, copied
byte-for-byte from the MCP's migrations.

## What was built

Ordered by commit (each left the tree build-able):

1. **Bootstrap** — `go.mod`, `Makefile`, `LICENSE`, `README.md`,
   `.env.example`, extended `.gitignore`, `.golangci.yml`, seed docs
   (`docs/architecture.md`, `docs/sources.md`, `docs/bigquery.md`,
   `docs/deploy.md`).
2. **Model / config / logging** — `internal/model` (Ticker VO matching
   the MCP's regex, RawEvent envelope, Fund/Price/Dividend/Fundamentals
   types), `internal/config` (env + .env loader with `INGEST_*` prefix,
   per-capability Validate* methods), `internal/logging` (slog JSON,
   run_id context helpers).
3. **brapi adapter** — `internal/sources/brapi`: rate-limited HTTP
   client with retries on 429/5xx, iter.Seq2 streams for fund list,
   OHLCV, dividends, and fundamentals. Full unit suite against fixtures
   under `testdata/fixtures/`.
4. **CVM adapter** — `internal/sources/cvm`: HTTPS downloader for
   `inf_mensal_fii_YYYY.zip`, tolerant CSV parser (accent folding,
   Latin-1 → UTF-8 transcoding), emits `model.CVMInformeMensal` stream.
   Unit tests exercise a synthetic ZIP and the Latin-1 path.
5. **BigQuery client + schema + land + read** — `internal/bq` with
   generic writers (`LandPrices`, `LandDividends`, `LandFundamentals`,
   `LandFunds`, `LandCVMInforme`), `bigquery.StructSaver` + `civil.Date`
   for typed writes, and `Read[T]` using iter.Seq2 for streaming back.
6. **BQ SQL transforms** — embedded `internal/bq/sql/*.sql`:
   `01_create_raw_tables.sql`, `02_create_canon_tables.sql`,
   `10_transform_funds.sql`, `11_transform_prices.sql`,
   `12_transform_dividends.sql`, `13_transform_fundamentals.sql`,
   `20_materialize_snapshots.sql`. All canon transforms are idempotent
   `MERGE`s that pick the latest `ingested_at` per logical PK. Snapshot
   uses `CREATE OR REPLACE TABLE` so it is safe to re-run.
7. **SQLite export + contract test** — `internal/export` streams
   canon → SQLite via batched transactions with prepared statements,
   applies the copied MCP migrations, and runs `VACUUM` for a compact
   output. `export_test.go` mirrors every query the MCP repository
   executes (`internal/sqlite/*.go`) and asserts they succeed against
   the generated DB. Also asserts byte-parity with the MCP migrations.
8. **Publish** — `internal/publish`: GCS uploader (`latest/`, dated
   `history/`, `metadata.json` sidecar, per-run reports under `runs/`)
   and GitHub releases via `go-github/v66` for the monthly snapshot.
9. **Pipeline orchestrator** — `internal/pipeline`: composes fetch →
   land → transform → export → publish. Emits per-stage `StageResult`
   and an aggregate `RunReport` serialised as JSON. Quality gate aborts
   publish if `fund_count < INGEST_MIN_FUND_COUNT`.
10. **CLI** — `internal/cli` + `cmd/reitbrazilctl/main.go` built on
    `cobra` + `charmbracelet/fang`. Subcommands: `doctor`, `discover`,
    `fetch prices|dividends|fundamentals|cvm`, `transform --stage`,
    `export sqlite`, `publish gcs|release`, `run daily|monthly`. Signal
    handling and `run_id` are wired at `main`.
11. **Dockerfile + Cloud Build** — distroless multi-stage Dockerfile,
    `.dockerignore`, `deploy/cloudbuild.yaml` that pushes tagged images
    to Artifact Registry.
12. **Terraform** — `deploy/terraform/` provisions BigQuery datasets,
    GCS bucket (versioning + 365d lifecycle on `history/`), Artifact
    Registry repo, Secret Manager secrets (`brapi-token`,
    `github-token`), least-privilege runner SA + scheduler SA, two
    Cloud Run Jobs, and two Cloud Scheduler triggers. Passes
    `terraform validate` and `terraform fmt -check -recursive`.
13. **Docs / CI / SUMMARY** — `README.md`, `docs/*.md`, `.github/workflows`
    for unit + lint + Terraform (always) and a gated `integration.yml`
    for live tests.

## What's deferred (explicit out-of-scope, as the prompt instructed)

- **B3 COTAHIST parser** — skeleton intentionally not created; left as
  a `// TODO(v1.1)` noted in `docs/sources.md`. Adding it is a drop-in
  under `internal/sources/b3`.
- **Cloud Monitoring alerts** — documented in `docs/deploy.md` as the
  next step. The pipeline already writes run reports to
  `gs://reitbrazil-db/runs/`, so a log-based alert is a single
  Terraform addition when it's time.
- **FI-Infra / CRI / LCI coverage**, **`refresh_data` MCP tool**,
  **Looker Studio dashboard**, **Postgres sink**, **chaos/fault tests**
  — all documented as v1.1+.

## How to run locally

```bash
cp .env.example .env
# fill in INGEST_BRAPI_TOKEN and GOOGLE_APPLICATION_CREDENTIALS
make build
./bin/reitbrazilctl doctor
./bin/reitbrazilctl fetch prices --tickers XPLG11,HGLG11 --from 2026-01-01
./bin/reitbrazilctl transform --stage prices
./bin/reitbrazilctl export sqlite --output ./out/reitbrazil.db
```

Full pipeline:

```bash
make run-daily
# or, for the CVM + GitHub release monthly flow:
make run-monthly
```

Run the MCP server against the generated DB:

```bash
REITBR_DB_PATH=$(pwd)/out/reitbrazil.db \
  /Users/addo/jobs/addodelgrossi/reitbrazil/bin/reitbrazil
```

## How to do the first deploy

```bash
cd deploy/terraform
terraform init
terraform apply -var="project_id=reitbrazil"

# seed secrets
echo -n "$BRAPI_TOKEN" | gcloud secrets versions add brapi-token \
    --project=reitbrazil --data-file=-
echo -n "$GITHUB_TOKEN" | gcloud secrets versions add github-token \
    --project=reitbrazil --data-file=-

# build + push image
cd ../..
make docker-build docker-push DOCKER_TAG=$(git rev-parse --short HEAD)

# point the jobs at the new image
cd deploy/terraform
terraform apply -var="project_id=reitbrazil" \
                -var="image_tag=$(git -C ../.. rev-parse --short HEAD)"

# smoke-test
gcloud run jobs execute reitbrazil-sync-daily --region=southamerica-east1
gsutil ls gs://reitbrazil-db/latest/
```

## Verification

- `make test` — unit + contract pass.
- `make lint` — clean (falls back to `go vet` if golangci-lint is
  missing locally).
- `./bin/reitbrazilctl --help` — shows every subcommand, global flags,
  version, and styled output via `fang`.
- `terraform validate` + `terraform fmt -check -recursive` inside
  `deploy/terraform/` — both succeed.
- Contract test in `internal/export/export_test.go` asserts migration
  byte-parity with the MCP server and replays its queries against the
  generated DB.

## Invariants upheld

1. SQLite schema copied byte-for-byte from the MCP repo.
2. Transforms are idempotent (MERGE by logical PK on latest
   `ingested_at`; snapshots use `CREATE OR REPLACE TABLE`).
3. Raw layer is append-only — no UPDATE/DELETE paths in any SQL file.
4. Secrets live only in `.env` (gitignored) or Secret Manager.
5. Export orders deterministically (`ORDER BY ticker ASC, trade_date ASC`
   etc.).
6. Quality gate aborts publish if `fund_count` falls below
   `INGEST_MIN_FUND_COUNT` (default 100).
