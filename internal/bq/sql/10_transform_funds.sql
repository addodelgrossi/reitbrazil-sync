-- 10_transform_funds.sql
-- Dedupe the fund list by latest ingested_at and upsert into canon.funds.
-- CVM informe payload may supply CNPJ; brapi supplies name+segment+mandate.
-- We prefer brapi rows as the source of record for the registry because
-- they include the ticker.

MERGE `${project}.${dataset_canon}.funds` T
USING (
  WITH brapi_latest AS (
    SELECT *
    FROM (
      SELECT
        ticker,
        long_name,
        short_name,
        segment,
        mandate,
        ingested_at,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ingested_at DESC) AS rn
      FROM `${project}.${dataset_raw}.brapi_fund_list`
    )
    WHERE rn = 1
  ),
  cvm_latest AS (
    SELECT ticker, ANY_VALUE(cnpj) AS cnpj
    FROM `${project}.${dataset_raw}.cvm_informe_mensal`
    WHERE ticker IS NOT NULL AND ticker != ''
    GROUP BY ticker
  )
  SELECT
    b.ticker,
    c.cnpj AS cnpj,
    COALESCE(NULLIF(b.long_name, ''), b.short_name, b.ticker) AS name,
    b.segment,
    b.mandate,
    CAST(NULL AS STRING) AS manager,
    CAST(NULL AS STRING) AS administrator,
    CAST(NULL AS DATE)   AS ipo_date,
    TRUE                 AS listed
  FROM brapi_latest b
  LEFT JOIN cvm_latest c USING (ticker)
) S
ON T.ticker = S.ticker
WHEN MATCHED AND (
    COALESCE(T.name, '')          != COALESCE(S.name, '') OR
    COALESCE(T.segment, '')       != COALESCE(S.segment, '') OR
    COALESCE(T.mandate, '')       != COALESCE(S.mandate, '') OR
    COALESCE(T.cnpj, '')          != COALESCE(S.cnpj, '') OR
    T.listed != S.listed
)
THEN UPDATE SET
  name          = S.name,
  segment       = S.segment,
  mandate       = S.mandate,
  cnpj          = COALESCE(S.cnpj, T.cnpj),
  manager       = COALESCE(S.manager, T.manager),
  administrator = COALESCE(S.administrator, T.administrator),
  ipo_date      = COALESCE(S.ipo_date, T.ipo_date),
  listed        = S.listed
WHEN NOT MATCHED THEN INSERT (
  ticker, cnpj, name, segment, mandate, manager, administrator, ipo_date, listed
) VALUES (
  S.ticker, S.cnpj, S.name, S.segment, S.mandate, S.manager, S.administrator, S.ipo_date, S.listed
);
