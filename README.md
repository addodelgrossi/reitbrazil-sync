# reitbrazil-sync

Data pipeline for collecting, processing and building the SQLite database
consumed by the [`reitbrazil`](https://github.com/addodelgrossi/reitbrazil)
MCP server.

`reitbrazilctl` is a single Go CLI that:

1. Fetches real Brazilian REIT (FII) data from stable, legal sources
   (brapi.dev Pro, CVM open data).
2. Lands raw payloads in **BigQuery** (`reitbrazil_raw`, append-only).
3. Transforms them into canonical tables (`reitbrazil_canon`, idempotent
   MERGE statements).
4. Exports a **SQLite** file (`reitbrazil.db`) with the exact schema
   consumed by the MCP server.
5. Publishes the SQLite to **GCS** (`gs://reitbrazil-db/latest/`) daily
   and cuts a monthly **GitHub Release** as an auditable snapshot.

The MCP server is strictly a consumer of `reitbrazil.db`. The contract
between the two projects is the SQLite schema, nothing else.

## Quick start

```bash
# 1. Clone and set up
git clone git@github.com:addodelgrossi/reitbrazil-sync.git
cd reitbrazil-sync
cp .env.example .env
# edit .env with real brapi token + GCP credentials

# 2. Build
make build

# 3. Preflight
./bin/reitbrazilctl doctor

# 4. Manual pipeline run (local)
./bin/reitbrazilctl fetch prices --tickers XPLG11,HGLG11 --from 2026-01-01
./bin/reitbrazilctl transform --stage prices
./bin/reitbrazilctl export sqlite --output ./out/reitbrazil.db

# 5. Full daily run
make run-daily
```

## Architecture

Four stages with clean boundaries:

```
fetch (sources/) → land (bq.raw) → transform (bq.canon via MERGE) → export (sqlite) → publish (gcs/github)
```

- **fetch** knows HTTP, not BigQuery. Emits typed events.
- **land** knows BigQuery, not HTTP. Writes append-only raw rows with
  `ingested_at`.
- **transform** is pure SQL, embedded via `//go:embed`, idempotent
  (`MERGE` by logical PK on latest `ingested_at`).
- **export** streams canon rows into a fresh SQLite file, applying the
  MCP migrations unchanged.
- **publish** uploads to GCS (daily) and optionally cuts a GitHub
  release (monthly).

More detail:

- [docs/architecture.md](docs/architecture.md) — stages, data flow,
  isolation guarantees.
- [docs/sources.md](docs/sources.md) — brapi, CVM, B3 COTAHIST (v1.1).
- [docs/bigquery.md](docs/bigquery.md) — table layout and idempotency
  contract.
- [docs/deploy.md](docs/deploy.md) — Docker, Terraform, Cloud Run Job,
  Cloud Scheduler.

## CLI

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

Global flags: `--log-level`, `--log-format`, `--config`, `--project-id`,
`--dry-run`.

## Invariants

1. **Schema parity with the MCP.** Migrations in `internal/export/migrations_sqlite/`
   are copied verbatim from `reitbrazil/internal/storage/migrations/`.
   Any change must go to both repos simultaneously.
2. **Transforms are idempotent.** Running `run daily` N times produces
   identical canon rows.
3. **Raw is immutable.** Never `UPDATE`/`DELETE` in `reitbrazil_raw.*`.
4. **Secrets out of git.** `.env` is gitignored; production reads from
   Secret Manager.
5. **Quality gate.** If `fund_count < INGEST_MIN_FUND_COUNT` or max
   `trade_date` is older than `INGEST_MAX_PRICE_LAG_DAYS`, publishing is
   aborted and a run report is written to
   `gs://reitbrazil-db/runs/error-YYYY-MM-DD.json`.

## Scheduling (production)

- Daily: Cloud Run Job `reitbrazil-sync-daily`, cron `15 22 * * 1-5`
  (22:15 São Paulo, weekdays).
- Monthly: Cloud Run Job `reitbrazil-sync-monthly`, cron `30 23 2 * *`
  (day 2, 23:30 São Paulo) — publishes GitHub release.

See [docs/deploy.md](docs/deploy.md) for full Terraform walkthrough.

## License

MIT. See [LICENSE](LICENSE).
