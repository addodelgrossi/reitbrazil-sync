-- 20_materialize_snapshots.sql
-- Rebuilds canon.fund_snapshots with rolling aggregates used by the MCP
-- screener plus BigQuery-only analytical fields. Recomputed in full every run.

CREATE OR REPLACE TABLE `${project}.${dataset_canon}.fund_snapshots`
PARTITION BY DATE(updated_at)
CLUSTER BY segment, ticker
AS
WITH last_px AS (
  SELECT ticker, MAX(trade_date) AS last_date
  FROM `${project}.${dataset_canon}.prices`
  GROUP BY ticker
),
last_close AS (
  SELECT p.ticker, p.close AS last_close, p.trade_date AS last_close_date
  FROM `${project}.${dataset_canon}.prices` p
  JOIN last_px l ON p.ticker = l.ticker AND p.trade_date = l.last_date
),
recent_prices AS (
  SELECT ticker, trade_date, close, volume
  FROM `${project}.${dataset_canon}.prices`
  WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY)
),
price_returns AS (
  SELECT
    ticker,
    trade_date,
    close,
    volume,
    SAFE_DIVIDE(close, LAG(close) OVER (PARTITION BY ticker ORDER BY trade_date)) - 1 AS daily_return
  FROM recent_prices
),
vol90 AS (
  SELECT
    ticker,
    AVG(volume) AS avg_daily_volume_90d,
    AVG(volume * close) AS avg_daily_traded_value_90d_brl,
    STDDEV(daily_return) AS daily_vol
  FROM price_returns
  GROUP BY ticker
),
drawdown AS (
  SELECT
    ticker,
    MIN(SAFE_DIVIDE(close, max_close) - 1) AS max_drawdown_1y
  FROM (
    SELECT
      ticker,
      trade_date,
      close,
      MAX(close) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS max_close
    FROM `${project}.${dataset_canon}.prices`
    WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  )
  GROUP BY ticker
),
distributions_12m AS (
  SELECT
    ticker,
    SUM(amount_per_share) AS sum_12m,
    COUNT(DISTINCT DATE_TRUNC(ex_date, MONTH)) AS months_paid_12m,
    STDDEV(amount_per_share) AS distribution_stddev_12m
  FROM `${project}.${dataset_canon}.dividends`
  WHERE kind = 'dividend'
    AND ex_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
  GROUP BY ticker
),
distributions_6m AS (
  SELECT ticker, SUM(amount_per_share) AS sum_6m
  FROM `${project}.${dataset_canon}.dividends`
  WHERE kind = 'dividend'
    AND ex_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND CURRENT_DATE()
  GROUP BY ticker
),
distributions_3m AS (
  SELECT ticker, SUM(amount_per_share) AS sum_3m
  FROM `${project}.${dataset_canon}.dividends`
  WHERE kind = 'dividend'
    AND ex_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
  GROUP BY ticker
),
last_distribution AS (
  SELECT ticker, amount_per_share, ex_date
  FROM (
    SELECT
      ticker,
      amount_per_share,
      ex_date,
      ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ex_date DESC, payment_date DESC) AS rn
    FROM `${project}.${dataset_canon}.dividends`
    WHERE kind = 'dividend'
  )
  WHERE rn = 1
),
latest_fund AS (
  SELECT f.ticker, f.segment, f.mandate
  FROM `${project}.${dataset_canon}.funds` f
),
latest_fundamental AS (
  SELECT *
  FROM (
    SELECT
      ticker,
      pvp,
      nav_per_share,
      as_of,
      ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY as_of DESC) AS rn
    FROM `${project}.${dataset_canon}.fundamentals`
  )
  WHERE rn = 1
)
SELECT
  f.ticker,
  lc.last_close,
  lc.last_close_date,
  SAFE_DIVIDE(d12.sum_12m, lc.last_close) AS dy_trailing_12m,
  SAFE_DIVIDE(d3.sum_3m * 4, lc.last_close) AS dy_forward_est,
  v.avg_daily_volume_90d,
  v.avg_daily_traded_value_90d_brl,
  v.daily_vol * SQRT(252) AS volatility_90d,
  dd.max_drawdown_1y,
  lf.pvp,
  SAFE_DIVIDE(d12.sum_12m, lc.last_close) AS dy_12m,
  SAFE_DIVIDE(d6.sum_6m, lc.last_close) AS dy_6m,
  SAFE_DIVIDE(d3.sum_3m * 4, lc.last_close) AS dy_3m_annualized,
  ld.amount_per_share AS last_distribution_amount,
  ld.ex_date AS last_distribution_date,
  d12.months_paid_12m,
  d12.distribution_stddev_12m,
  lf.pvp AS pvp_latest,
  lf.nav_per_share AS nav_per_share_latest,
  SAFE_DIVIDE(lc.last_close, lf.nav_per_share) - 1 AS discount_to_nav,
  f.segment,
  f.mandate,
  CURRENT_TIMESTAMP() AS updated_at
FROM latest_fund f
LEFT JOIN last_close lc          ON lc.ticker = f.ticker
LEFT JOIN distributions_12m d12  ON d12.ticker = f.ticker
LEFT JOIN distributions_6m d6    ON d6.ticker = f.ticker
LEFT JOIN distributions_3m d3    ON d3.ticker = f.ticker
LEFT JOIN last_distribution ld   ON ld.ticker = f.ticker
LEFT JOIN vol90 v                ON v.ticker = f.ticker
LEFT JOIN drawdown dd            ON dd.ticker = f.ticker
LEFT JOIN latest_fundamental lf  ON lf.ticker = f.ticker;
