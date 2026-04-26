-- 01_create_raw_tables.sql
-- Bronze layer. Append-only. Partitioned by DATE(ingested_at).
-- Safe to run on every startup (idempotent via CREATE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS `${project}.${dataset_raw}.brapi_fund_list` (
  ticker      STRING NOT NULL,
  cnpj        STRING,
  isin        STRING,
  short_name  STRING,
  long_name   STRING,
  segment     STRING,
  mandate     STRING,
  manager     STRING,
  administrator STRING,
  listed      BOOL,
  payload     JSON,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY ticker;

ALTER TABLE `${project}.${dataset_raw}.brapi_fund_list` ADD COLUMN IF NOT EXISTS cnpj STRING;
ALTER TABLE `${project}.${dataset_raw}.brapi_fund_list` ADD COLUMN IF NOT EXISTS isin STRING;
ALTER TABLE `${project}.${dataset_raw}.brapi_fund_list` ADD COLUMN IF NOT EXISTS manager STRING;
ALTER TABLE `${project}.${dataset_raw}.brapi_fund_list` ADD COLUMN IF NOT EXISTS administrator STRING;
ALTER TABLE `${project}.${dataset_raw}.brapi_fund_list` ADD COLUMN IF NOT EXISTS listed BOOL;

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
  event_id      STRING NOT NULL,
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

ALTER TABLE `${project}.${dataset_raw}.brapi_dividends` ADD COLUMN IF NOT EXISTS event_id STRING;

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
  cnpj                  STRING NOT NULL,
  ticker                STRING,
  reference_month       DATE NOT NULL,
  name                  STRING,
  isin                  STRING,
  segment               STRING,
  mandate               STRING,
  administrator         STRING,
  listed                BOOL,
  num_investors         INT64,
  assets_total          FLOAT64,
  equity_total          FLOAT64,
  shares_outstanding    FLOAT64,
  nav_per_share         FLOAT64,
  dividend_yield_month  FLOAT64,
  amortization_month    FLOAT64,
  vacancy_physical      FLOAT64,
  vacancy_financial     FLOAT64,
  real_estate_total     FLOAT64,
  financial_asset_total FLOAT64,
  payload               JSON,
  ingested_at           TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY cnpj, reference_month;

ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS name STRING;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS isin STRING;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS segment STRING;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS mandate STRING;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS administrator STRING;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS listed BOOL;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS assets_total FLOAT64;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS shares_outstanding FLOAT64;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS dividend_yield_month FLOAT64;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS amortization_month FLOAT64;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS real_estate_total FLOAT64;
ALTER TABLE `${project}.${dataset_raw}.cvm_informe_mensal` ADD COLUMN IF NOT EXISTS financial_asset_total FLOAT64;
