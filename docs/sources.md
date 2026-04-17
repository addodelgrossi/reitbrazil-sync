# Data Sources

## brapi.dev (primary, daily)

- Base URL: `https://brapi.dev/api`
- Auth: `Authorization: Bearer $INGEST_BRAPI_TOKEN`
- Plan: Pro (500k requests/month, FII coverage, dividends, fundamentals)

Endpoints used:

| Endpoint | Purpose | Cadence |
|---|---|---|
| `/quote/list?type=fund&limit=100&page=N` | Discover FII universe | daily |
| `/quote/{ticker}?range=5y&interval=1d` | OHLCV history | daily incremental |
| `/quote/{ticker}?dividends=true` | Dividend events | daily |
| `/quote/{ticker}?modules=defaultKeyStatistics,financialData` | Fundamentals | daily |

Rate limit: `INGEST_RATE_LIMIT_RPS` (default 3 rps = ~258k req/day,
plenty of headroom).

Retries: 3 attempts with exponential backoff, base 500ms, on 429 and
5xx only. No retry on 4xx other than 429.

## CVM (monthly, authoritative fundamentals)

- Portal: https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/
- Files: ZIP containing CSVs, one per month, covering ~all listed FIIs.
- License: open data.

Fields we extract: `cnpj`, `ticker` (when present), `reference_month`,
`num_investors`, `equity_total`, `nav_per_share`, `vacancy_physical`,
`vacancy_financial`.

CVM publishes on a slight delay (~15 days after month close). The
monthly pipeline runs on day 2 of each month to catch the prior month
if available, falling back to last-known otherwise.

## B3 COTAHIST (optional, v1.1)

Historical daily OHLCV from B3 in a fixed 245-byte record layout.
Useful for backfill beyond what brapi exposes. Not required for v1.

## investidor10.com.br (excluded)

No public API, ToS prohibits scraping. Not used.
