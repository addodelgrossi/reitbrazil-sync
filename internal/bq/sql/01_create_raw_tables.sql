-- 01_create_raw_tables.sql
-- Bronze layer. Append-only. Partitioned by DATE(ingested_at).
-- Safe to run on every startup (idempotent via CREATE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS `${project}.${dataset_raw}.brapi_fund_list` (
  ticker      STRING NOT NULL,
  short_name  STRING,
  long_name   STRING,
  segment     STRING,
  mandate     STRING,
  payload     JSON,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY ticker;

CREATE TABLE IF NOT EXISTS `${project}.${dataset_raw}.brapi_quote` (
  ticker      STRING NOT NULL,
  trade_date  DATE NOT NULL,
  open        FLOAT64,
  high        FLOAT64,
  low         FLOAT64,
  close       FLOAT64 NOT NULL,
  volume      INT64,
  payload     JSON,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY ticker, trade_date;

CREATE TABLE IF NOT EXISTS `${project}.${dataset_raw}.brapi_dividends` (
  ticker        STRING NOT NULL,
  ex_date       DATE NOT NULL,
  announce_date DATE,
  record_date   DATE,
  payment_date  DATE,
  amount        FLOAT64 NOT NULL,
  kind          STRING NOT NULL,
  source        STRING,
  payload       JSON,
  ingested_at   TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY ticker, ex_date;

CREATE TABLE IF NOT EXISTS `${project}.${dataset_raw}.brapi_fundamentals` (
  ticker             STRING NOT NULL,
  as_of              DATE NOT NULL,
  nav_per_share      FLOAT64,
  pvp                FLOAT64,
  assets_total       FLOAT64,
  equity_total       FLOAT64,
  num_investors      INT64,
  liquidity_90d      FLOAT64,
  vacancy_physical   FLOAT64,
  vacancy_financial  FLOAT64,
  occupancy_rate     FLOAT64,
  payload            JSON,
  ingested_at        TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY ticker;

CREATE TABLE IF NOT EXISTS `${project}.${dataset_raw}.cvm_informe_mensal` (
  cnpj              STRING NOT NULL,
  ticker            STRING,
  reference_month   DATE NOT NULL,
  num_investors     INT64,
  equity_total      FLOAT64,
  nav_per_share     FLOAT64,
  vacancy_physical  FLOAT64,
  vacancy_financial FLOAT64,
  payload           JSON,
  ingested_at       TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY cnpj, reference_month;
