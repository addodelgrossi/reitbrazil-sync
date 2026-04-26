-- 13_transform_fundamentals.sql
-- Dedupe fundamentals by (ticker, as_of) on latest ingested_at. We
-- combine brapi (NAV/PVP/assets) with CVM informes (num_investors,
-- equity_total, NAV, vacancy_*) by referencing the same as_of grain
-- (month end for CVM, day granularity for brapi).

MERGE `${project}.${dataset_canon}.fundamentals` T
USING (
  WITH brapi_f AS (
    SELECT *
    FROM (
      SELECT
        ticker, as_of, nav_per_share, pvp, assets_total, equity_total,
        num_investors, liquidity_90d, vacancy_physical, vacancy_financial,
        occupancy_rate, ingested_at,
        ROW_NUMBER() OVER (PARTITION BY ticker, as_of ORDER BY ingested_at DESC) AS rn
      FROM `${project}.${dataset_raw}.brapi_fundamentals`
    )
    WHERE rn = 1
  ),
  cvm_f AS (
    SELECT *
    FROM (
      SELECT
        ticker,
        reference_month AS as_of,
        num_investors,
        assets_total,
        equity_total,
        nav_per_share,
        vacancy_physical,
        vacancy_financial,
        ingested_at,
        ROW_NUMBER() OVER (PARTITION BY ticker, reference_month ORDER BY ingested_at DESC) AS rn
      FROM `${project}.${dataset_raw}.cvm_informe_mensal`
      WHERE ticker IS NOT NULL AND ticker != ''
    )
    WHERE rn = 1
  ),
  combined AS (
    SELECT
      COALESCE(b.ticker, c.ticker) AS ticker,
      COALESCE(b.as_of,  c.as_of)  AS as_of,
      COALESCE(b.nav_per_share,      c.nav_per_share)      AS nav_per_share,
      b.pvp                                                  AS pvp,
      COALESCE(b.assets_total,       c.assets_total)         AS assets_total,
      COALESCE(b.equity_total,       c.equity_total)        AS equity_total,
      COALESCE(b.num_investors,      c.num_investors)       AS num_investors,
      b.liquidity_90d                                        AS liquidity_90d,
      COALESCE(b.vacancy_physical,   c.vacancy_physical)    AS vacancy_physical,
      COALESCE(b.vacancy_financial,  c.vacancy_financial)   AS vacancy_financial,
      b.occupancy_rate                                       AS occupancy_rate
    FROM brapi_f b
    FULL OUTER JOIN cvm_f c USING (ticker, as_of)
  )
  SELECT * FROM combined
) S
ON T.ticker = S.ticker AND T.as_of = S.as_of
WHEN MATCHED AND (
     COALESCE(T.nav_per_share,     -1) != COALESCE(S.nav_per_share,     -1) OR
     COALESCE(T.pvp,               -1) != COALESCE(S.pvp,               -1) OR
     COALESCE(T.equity_total,      -1) != COALESCE(S.equity_total,      -1) OR
     COALESCE(T.num_investors,     -1) != COALESCE(S.num_investors,     -1) OR
     COALESCE(T.vacancy_physical,  -1) != COALESCE(S.vacancy_physical,  -1) OR
     COALESCE(T.vacancy_financial, -1) != COALESCE(S.vacancy_financial, -1)
)
THEN UPDATE SET
  nav_per_share     = S.nav_per_share,
  pvp               = S.pvp,
  assets_total      = S.assets_total,
  equity_total      = S.equity_total,
  num_investors     = S.num_investors,
  liquidity_90d     = S.liquidity_90d,
  vacancy_physical  = S.vacancy_physical,
  vacancy_financial = S.vacancy_financial,
  occupancy_rate    = S.occupancy_rate
WHEN NOT MATCHED THEN INSERT (
  ticker, as_of, nav_per_share, pvp, assets_total, equity_total,
  num_investors, liquidity_90d, vacancy_physical, vacancy_financial, occupancy_rate
) VALUES (
  S.ticker, S.as_of, S.nav_per_share, S.pvp, S.assets_total, S.equity_total,
  S.num_investors, S.liquidity_90d, S.vacancy_physical, S.vacancy_financial, S.occupancy_rate
);
