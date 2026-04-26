-- 12_transform_dividends.sql
-- Dedupe dividend events by stable event_id on latest
-- ingested_at, then MERGE into canon.dividends.

MERGE `${project}.${dataset_canon}.dividends` T
USING (
  SELECT event_id, ticker, ex_date, announce_date, record_date, payment_date,
         amount AS amount_per_share, kind, source
  FROM (
    SELECT
      COALESCE(
        event_id,
        FORMAT('%s:%s:%s:%s:%.12g:%s',
          ticker,
          CAST(ex_date AS STRING),
          COALESCE(CAST(payment_date AS STRING), ''),
          kind,
          amount,
          COALESCE(source, 'unknown'))
      ) AS event_id,
      ticker, ex_date, announce_date, record_date, payment_date,
      amount, kind, source, ingested_at,
      ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY ingested_at DESC
      ) AS rn
    FROM `${project}.${dataset_raw}.brapi_dividends`
  )
  WHERE rn = 1
) S
ON COALESCE(
     T.event_id,
     FORMAT('%s:%s:%s:%s:%.12g:%s',
       T.ticker,
       CAST(T.ex_date AS STRING),
       COALESCE(CAST(T.payment_date AS STRING), ''),
       T.kind,
       T.amount_per_share,
       COALESCE(T.source, 'unknown'))
   ) = S.event_id
WHEN MATCHED AND (
     COALESCE(T.amount_per_share, -1) != COALESCE(S.amount_per_share, -1) OR
     COALESCE(CAST(T.payment_date  AS STRING), '') != COALESCE(CAST(S.payment_date  AS STRING), '') OR
     COALESCE(CAST(T.record_date   AS STRING), '') != COALESCE(CAST(S.record_date   AS STRING), '') OR
     COALESCE(CAST(T.announce_date AS STRING), '') != COALESCE(CAST(S.announce_date AS STRING), '') OR
     COALESCE(T.source, '') != COALESCE(S.source, '')
)
THEN UPDATE SET
  ticker           = S.ticker,
  event_id         = S.event_id,
  announce_date    = S.announce_date,
  ex_date          = S.ex_date,
  record_date      = S.record_date,
  payment_date     = S.payment_date,
  amount_per_share = S.amount_per_share,
  kind             = S.kind,
  source           = S.source
WHEN NOT MATCHED THEN INSERT (
  event_id, ticker, announce_date, ex_date, record_date, payment_date,
  amount_per_share, kind, source
) VALUES (
  S.event_id, S.ticker, S.announce_date, S.ex_date, S.record_date, S.payment_date,
  S.amount_per_share, S.kind, S.source
);
