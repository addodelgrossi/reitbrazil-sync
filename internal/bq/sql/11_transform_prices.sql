-- 11_transform_prices.sql
-- Dedupe OHLCV bars by (ticker, trade_date) on latest ingested_at, then
-- MERGE into canon.prices. Idempotent.

MERGE `${project}.${dataset_canon}.prices` T
USING (
  SELECT ticker, trade_date, open, high, low, close, volume
  FROM (
    SELECT
      ticker, trade_date, open, high, low, close, volume, ingested_at,
      ROW_NUMBER() OVER (
        PARTITION BY ticker, trade_date
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `${project}.${dataset_raw}.brapi_quote`
  )
  WHERE rn = 1
) S
ON T.ticker = S.ticker AND T.trade_date = S.trade_date
WHEN MATCHED AND (
     COALESCE(T.open,   -1) != COALESCE(S.open,   -1) OR
     COALESCE(T.high,   -1) != COALESCE(S.high,   -1) OR
     COALESCE(T.low,    -1) != COALESCE(S.low,    -1) OR
     COALESCE(T.close,  -1) != COALESCE(S.close,  -1) OR
     COALESCE(T.volume, -1) != COALESCE(S.volume, -1)
)
THEN UPDATE SET
  open   = S.open,
  high   = S.high,
  low    = S.low,
  close  = S.close,
  volume = S.volume
WHEN NOT MATCHED THEN INSERT (
  ticker, trade_date, open, high, low, close, volume
) VALUES (
  S.ticker, S.trade_date, S.open, S.high, S.low, S.close, S.volume
);
