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

## CVM CAD_FI (legacy registry — not used)

Portal still publishes `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv`,
but under Res. CVM 175 (Oct 2023) FIIs moved out of this cadastro. Only
a handful of pre-Res175 FIIs remain there with `SIT = EM FUNCIONAMENTO
NORMAL` — the modern universe (~720 B3-listed funds) lives in the
informe mensal instead.

## B3 COTAHIST (optional, v1.1)

Historical daily OHLCV from B3 in a fixed 245-byte record layout.
Useful for backfill beyond what brapi exposes. Not required for v1.

## Diagnostics: `reitbrazilctl coverage`

Read-only command that compares brapi's fund universe against the CVM
informe mensal. It is not part of any scheduled pipeline — run it
manually to decide whether brapi is missing FIIs.

Two inputs:

1. `brapi /quote/list?type=fund` — tickers brapi exposes.
2. `CVM inf_mensal_fii_YYYY.zip` (default: previous calendar year). The
   command only parses `inf_mensal_fii_geral_YYYY.csv` inside the ZIP,
   which carries the Res-175 schema (`CNPJ_Fundo_Classe`,
   `Nome_Fundo_Classe`, `Codigo_ISIN`, `Mandato`, `Segmento_Atuacao`,
   `Mercado_Negociacao_Bolsa`).

Ticker bridge: ISINs matching `BR<4-letter>CTF<digits>` yield a ticker
of the form `<4-letter>11`. Non-conforming ISINs (legacy codes, empty)
fall into `cvm_b3_listed_without_ticker` instead.

| Count | Meaning |
|---|---|
| `cvm_total_cnpjs` | Distinct CNPJs filing the informe in the chosen year. |
| `cvm_b3_listed` | Subset flagged `Mercado_Negociacao_Bolsa = S`. |
| `cvm_b3_listed_with_ticker` | Of those, CNPJs whose ISIN yields a valid 6-char ticker. |
| `brapi_total` | Funds returned by `/quote/list?type=fund`. |
| `brapi_matched` | Brapi tickers present in `cvm_b3_listed_with_ticker`. |
| `brapi_orphans` | Brapi tickers with no CVM match (likely ETFs, misclassified, stale). |
| `cvm_missing_from_brapi` | B3-listed FIIs brapi doesn't return — the **coverage gap**. |
| `cvm_no_ticker_from_isin` | B3-listed FIIs whose ISIN we can't map to a ticker. |

Flags: `--format=table\|json`, `--informe-year=YYYY`, `--dry-run`.

## investidor10.com.br (excluded)

No public API, ToS prohibits scraping. Not used.
