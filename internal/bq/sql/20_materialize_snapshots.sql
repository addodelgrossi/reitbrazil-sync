-- 20_materialize_snapshots.sql
-- Rebuilds canon.fund_snapshots with rolling aggregates used by the MCP
-- screener. Recomputed in full every run; the table is effectively a
-- materialised view. CURRENT_TIMESTAMP() is used for updated_at.

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
dy_trailing AS (
  SELECT
    d.ticker,
    SUM(d.amount_per_share) AS sum_12m
  FROM `${project}.${dataset_canon}.dividends` d
  WHERE d.kind = 'dividend'
    AND d.ex_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
  GROUP BY d.ticker
),
vol90 AS (
  SELECT
    ticker,
    AVG(volume * close) AS avg_daily_volume_90d,
    STDDEV(SAFE_DIVIDE(close, LAG(close) OVER (PARTITION BY ticker ORDER BY trade_date)) - 1) AS daily_vol
  FROM (
    SELECT *
    FROM `${project}.${dataset_canon}.prices`
    WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY)
  )
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
latest_fund AS (
  SELECT f.ticker, f.segment, f.mandate
  FROM `${project}.${dataset_canon}.funds` f
),
latest_pvp AS (
  SELECT *
  FROM (
    SELECT
      ticker, pvp, as_of,
      ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY as_of DESC) AS rn
    FROM `${project}.${dataset_canon}.fundamentals`
  )
  WHERE rn = 1
)
SELECT
  f.ticker,
  lc.last_close,
  lc.last_close_date,
  SAFE_DIVIDE(dy.sum_12m, lc.last_close) AS dy_trailing_12m,
  -- Forward estimate: last 3-month annualised run-rate.
  SAFE_DIVIDE(
    (SELECT SUM(amount_per_share)
     FROM `${project}.${dataset_canon}.dividends` d3
     WHERE d3.ticker = f.ticker
       AND d3.kind = 'dividend'
       AND d3.ex_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
    ) * 4,
    lc.last_close
  ) AS dy_forward_est,
  v.avg_daily_volume_90d,
  v.daily_vol * SQRT(252) AS volatility_90d,
  dd.max_drawdown_1y,
  fpvp.pvp,
  f.segment,
  f.mandate,
  CURRENT_TIMESTAMP() AS updated_at
FROM latest_fund f
LEFT JOIN last_close lc  ON lc.ticker = f.ticker
LEFT JOIN dy_trailing dy ON dy.ticker = f.ticker
LEFT JOIN vol90 v        ON v.ticker  = f.ticker
LEFT JOIN drawdown dd    ON dd.ticker = f.ticker
LEFT JOIN latest_pvp fpvp ON fpvp.ticker = f.ticker;
