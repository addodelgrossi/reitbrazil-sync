-- 12_transform_dividends.sql
-- Dedupe dividend events by (ticker, ex_date, kind) on latest
-- ingested_at, then MERGE into canon.dividends.

MERGE `${project}.${dataset_canon}.dividends` T
USING (
  SELECT ticker, ex_date, announce_date, record_date, payment_date,
         amount AS amount_per_share, kind, source
  FROM (
    SELECT
      ticker, ex_date, announce_date, record_date, payment_date,
      amount, kind, source, ingested_at,
      ROW_NUMBER() OVER (
        PARTITION BY ticker, ex_date, kind
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `${project}.${dataset_raw}.brapi_dividends`
  )
  WHERE rn = 1
) S
ON  T.ticker = S.ticker
AND T.ex_date = S.ex_date
AND T.kind   = S.kind
WHEN MATCHED AND (
     COALESCE(T.amount_per_share, -1) != COALESCE(S.amount_per_share, -1) OR
     COALESCE(CAST(T.payment_date  AS STRING), '') != COALESCE(CAST(S.payment_date  AS STRING), '') OR
     COALESCE(CAST(T.record_date   AS STRING), '') != COALESCE(CAST(S.record_date   AS STRING), '') OR
     COALESCE(CAST(T.announce_date AS STRING), '') != COALESCE(CAST(S.announce_date AS STRING), '')
)
THEN UPDATE SET
  announce_date    = S.announce_date,
  record_date      = S.record_date,
  payment_date     = S.payment_date,
  amount_per_share = S.amount_per_share,
  source           = S.source
WHEN NOT MATCHED THEN INSERT (
  ticker, announce_date, ex_date, record_date, payment_date,
  amount_per_share, kind, source
) VALUES (
  S.ticker, S.announce_date, S.ex_date, S.record_date, S.payment_date,
  S.amount_per_share, S.kind, S.source
);
