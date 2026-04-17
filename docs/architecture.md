# Architecture

`reitbrazil-sync` is a 4-stage pipeline with strict boundaries between
stages. No stage reaches across; each consumes a typed input and emits a
typed output.

```
┌─────────────────────────────────────────────────────────────┐
│ Stage 1 — Fetch                                             │
│   internal/sources/{brapi,cvm,b3}                           │
│   Input:  token, calendar, tickers                          │
│   Output: iter.Seq2[T, error] of typed events               │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│ Stage 2 — Land (Bronze)                                     │
│   internal/bq.Land → reitbrazil_raw.*                       │
│   Append-only, partitioned by DATE(ingested_at)             │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│ Stage 3 — Transform (Silver)                                │
│   bq_sql/1x_*.sql via internal/bq.RunTransforms             │
│   MERGE idempotente, dedupe por PK lógica, latest           │
│   ingested_at vence                                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│ Stage 4 — Export & Publish                                  │
│   internal/export  → reitbrazil.db (schema idêntico ao MCP) │
│   internal/publish → gs://reitbrazil-db/latest/             │
│   Mensal: GitHub release data-vYYYY.MM                      │
└─────────────────────────────────────────────────────────────┘
```

## Isolation

- `sources/*` does not import `bq`, `export`, `publish`.
- `bq` does not import `sources/*` — it receives `iter.Seq2[T, error]`.
- `export` depends on `bq.Read[T]` and the migrations; no knowledge of
  the upstream fetch.
- `publish` reads a file path; it does not care how the file was built.

## Idempotency

The raw layer is immutable (append-only). Each fetch appends a row with
`ingested_at = CURRENT_TIMESTAMP()`. Transforms select the latest
`ingested_at` per logical PK via `ROW_NUMBER() OVER (... ORDER BY
ingested_at DESC)` and `MERGE` into canon. Running a pipeline N times
produces identical canon rows.

## Contract with the MCP

The only contract is the SQLite schema. Migrations in
`internal/export/migrations_sqlite/` are kept byte-identical to those in
`github.com/addodelgrossi/reitbrazil/internal/storage/migrations/`.
`internal/export/contract_test.go` opens the generated DB and runs the
same queries the MCP executes, failing the build if any drift occurs.

## Observability

Every run carries a `run_id` UUID set at the top of the CLI command and
propagated via `context.Context` and slog attributes. Logs are emitted
as JSON to stderr so Cloud Logging ingests them without transformation.

On completion of `run daily`/`run monthly`, a run report is uploaded to
`gs://reitbrazil-db/runs/<YYYY-MM-DD>.json` with per-stage row counts,
durations, and errors.
