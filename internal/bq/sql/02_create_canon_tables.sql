-- 02_create_canon_tables.sql
-- Silver layer. Deduplicated by logical PK. MERGE targets.

CREATE TABLE IF NOT EXISTS `${project}.${dataset_canon}.funds` (
  ticker        STRING NOT NULL,
  cnpj          STRING,
  name          STRING NOT NULL,
  segment       STRING,
  mandate       STRING,
  manager       STRING,
  administrator STRING,
  ipo_date      DATE,
  listed        BOOL NOT NULL
)
CLUSTER BY segment, mandate, ticker;

CREATE TABLE IF NOT EXISTS `${project}.${dataset_canon}.prices` (
  ticker     STRING NOT NULL,
  trade_date DATE NOT NULL,
  open       FLOAT64,
  high       FLOAT64,
  low        FLOAT64,
  close      FLOAT64 NOT NULL,
  volume     INT64
)
PARTITION BY trade_date
CLUSTER BY ticker;

CREATE TABLE IF NOT EXISTS `${project}.${dataset_canon}.dividends` (
  ticker           STRING NOT NULL,
  announce_date    DATE,
  ex_date          DATE NOT NULL,
  record_date      DATE,
  payment_date     DATE,
  amount_per_share FLOAT64 NOT NULL,
  kind             STRING NOT NULL,
  source           STRING
)
PARTITION BY ex_date
CLUSTER BY ticker;

CREATE TABLE IF NOT EXISTS `${project}.${dataset_canon}.fundamentals` (
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
  occupancy_rate     FLOAT64
)
PARTITION BY as_of
CLUSTER BY ticker;

CREATE TABLE IF NOT EXISTS `${project}.${dataset_canon}.fund_snapshots` (
  ticker               STRING NOT NULL,
  last_close           FLOAT64,
  last_close_date      DATE,
  dy_trailing_12m      FLOAT64,
  dy_forward_est       FLOAT64,
  avg_daily_volume_90d FLOAT64,
  volatility_90d       FLOAT64,
  max_drawdown_1y      FLOAT64,
  pvp                  FLOAT64,
  segment              STRING,
  mandate              STRING,
  updated_at           TIMESTAMP NOT NULL
)
CLUSTER BY segment, ticker;
