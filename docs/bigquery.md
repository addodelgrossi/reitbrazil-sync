# BigQuery layout

Project: `reitbrazil`. Location: `southamerica-east1`.

Two datasets:

- `reitbrazil_raw` — bronze, append-only.
- `reitbrazil_canon` — silver, deduplicated, idempotent target.

All DDL and transforms are version-controlled under `bq_sql/` and
embedded in the binary via `//go:embed`.

## Raw tables (append-only, partitioned by DATE(ingested_at))

| Table | PK (logical) | Notes |
|---|---|---|
| `raw.brapi_fund_list` | `ticker` | Discovery snapshots |
| `raw.brapi_quote` | `(ticker, trade_date)` | OHLCV bars |
| `raw.brapi_dividends` | `(ticker, ex_date, kind)` | Distribution events |
| `raw.brapi_fundamentals` | `(ticker, as_of)` | Snapshot metrics |
| `raw.cvm_informe_mensal` | `(cnpj, reference_month)` | CVM monthly informe |

Each table has:

- `payload JSON` — the full upstream response, for auditability.
- `ingested_at TIMESTAMP NOT NULL` — partition key.

Never mutate raw rows. If a correction is needed, re-ingest with a newer
`ingested_at`.

## Canon tables (silver, deduplicated)

| Table | PK | Partition | Cluster |
|---|---|---|---|
| `canon.funds` | `ticker` | — | `(segment, mandate)` |
| `canon.prices` | `(ticker, trade_date)` | `trade_date` | `ticker` |
| `canon.dividends` | `(ticker, ex_date, kind)` | `ex_date` | `ticker` |
| `canon.fundamentals` | `(ticker, as_of)` | `as_of` | `ticker` |
| `canon.fund_snapshots` | `ticker` | — | `segment` |

## Transforms

All transforms are `MERGE` statements of the form:

```sql
MERGE `${project}.reitbrazil_canon.prices` T
USING (
  SELECT *
  FROM (
    SELECT ticker, trade_date, open, high, low, close, volume, ingested_at,
           ROW_NUMBER() OVER (PARTITION BY ticker, trade_date ORDER BY ingested_at DESC) AS rn
    FROM `${project}.reitbrazil_raw.brapi_quote`
  )
  WHERE rn = 1
) S
ON T.ticker = S.ticker AND T.trade_date = S.trade_date
WHEN MATCHED AND (T.close != S.close OR T.volume != S.volume)
  THEN UPDATE SET ...
WHEN NOT MATCHED
  THEN INSERT ...;
```

`${project}` is substituted at execution time.

The transforms are deliberately written to be safe to re-run: no
`TRUNCATE`, no `DELETE` outside of the `WHEN NOT MATCHED BY SOURCE`
branches used exclusively for dedupe of the canonical view.

## Snapshots

`canon.fund_snapshots` is rebuilt by `20_materialize_snapshots.sql`. It
computes rolling aggregates:

- `dy_trailing_12m` = sum(dividends over trailing 365d) / last close
- `dy_forward_est` = dy_trailing_12m * (1 + last 3m annualised growth)
- `avg_daily_volume_90d`
- `volatility_90d` (stddev of daily log-returns × √252)
- `max_drawdown_1y`
- `pvp` (from latest fundamentals)

Snapshots are recomputed each run; the SQLite consumer reads them as-is.
