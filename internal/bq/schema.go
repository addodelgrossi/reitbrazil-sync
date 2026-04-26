package bq

import (
	"cloud.google.com/go/bigquery"
)

// RawTable names for the bronze layer.
const (
	TableBrapiFundList     = "brapi_fund_list"
	TableBrapiQuote        = "brapi_quote"
	TableBrapiDividends    = "brapi_dividends"
	TableBrapiFundamentals = "brapi_fundamentals"
	TableCVMInformeMensal  = "cvm_informe_mensal"
)

// CanonTable names for the silver layer.
const (
	TableCanonFunds         = "funds"
	TableCanonPrices        = "prices"
	TableCanonDividends     = "dividends"
	TableCanonFundamentals  = "fundamentals"
	TableCanonFundSnapshots = "fund_snapshots"
)

// RawSchemas returns the schema definitions for every raw (bronze) table.
// The DDL in bq_sql/01_create_raw_tables.sql is authoritative for
// partitioning/clustering; this map exists so Land functions can build
// ValueSavers without reflection.
func RawSchemas() map[string]bigquery.Schema {
	return map[string]bigquery.Schema{
		TableBrapiFundList: {
			{Name: "ticker", Type: bigquery.StringFieldType, Required: true},
			{Name: "cnpj", Type: bigquery.StringFieldType},
			{Name: "isin", Type: bigquery.StringFieldType},
			{Name: "short_name", Type: bigquery.StringFieldType},
			{Name: "long_name", Type: bigquery.StringFieldType},
			{Name: "segment", Type: bigquery.StringFieldType},
			{Name: "mandate", Type: bigquery.StringFieldType},
			{Name: "manager", Type: bigquery.StringFieldType},
			{Name: "administrator", Type: bigquery.StringFieldType},
			{Name: "listed", Type: bigquery.BooleanFieldType},
			{Name: "payload", Type: bigquery.JSONFieldType},
			{Name: "ingested_at", Type: bigquery.TimestampFieldType, Required: true},
		},
		TableBrapiQuote: {
			{Name: "ticker", Type: bigquery.StringFieldType, Required: true},
			{Name: "trade_date", Type: bigquery.DateFieldType, Required: true},
			{Name: "open", Type: bigquery.FloatFieldType},
			{Name: "high", Type: bigquery.FloatFieldType},
			{Name: "low", Type: bigquery.FloatFieldType},
			{Name: "close", Type: bigquery.FloatFieldType, Required: true},
			{Name: "volume", Type: bigquery.IntegerFieldType},
			{Name: "payload", Type: bigquery.JSONFieldType},
			{Name: "ingested_at", Type: bigquery.TimestampFieldType, Required: true},
		},
		TableBrapiDividends: {
			{Name: "event_id", Type: bigquery.StringFieldType, Required: true},
			{Name: "ticker", Type: bigquery.StringFieldType, Required: true},
			{Name: "ex_date", Type: bigquery.DateFieldType, Required: true},
			{Name: "announce_date", Type: bigquery.DateFieldType},
			{Name: "record_date", Type: bigquery.DateFieldType},
			{Name: "payment_date", Type: bigquery.DateFieldType},
			{Name: "amount", Type: bigquery.FloatFieldType, Required: true},
			{Name: "kind", Type: bigquery.StringFieldType, Required: true},
			{Name: "source", Type: bigquery.StringFieldType},
			{Name: "payload", Type: bigquery.JSONFieldType},
			{Name: "ingested_at", Type: bigquery.TimestampFieldType, Required: true},
		},
		TableBrapiFundamentals: {
			{Name: "ticker", Type: bigquery.StringFieldType, Required: true},
			{Name: "as_of", Type: bigquery.DateFieldType, Required: true},
			{Name: "nav_per_share", Type: bigquery.FloatFieldType},
			{Name: "pvp", Type: bigquery.FloatFieldType},
			{Name: "assets_total", Type: bigquery.FloatFieldType},
			{Name: "equity_total", Type: bigquery.FloatFieldType},
			{Name: "num_investors", Type: bigquery.IntegerFieldType},
			{Name: "liquidity_90d", Type: bigquery.FloatFieldType},
			{Name: "vacancy_physical", Type: bigquery.FloatFieldType},
			{Name: "vacancy_financial", Type: bigquery.FloatFieldType},
			{Name: "occupancy_rate", Type: bigquery.FloatFieldType},
			{Name: "payload", Type: bigquery.JSONFieldType},
			{Name: "ingested_at", Type: bigquery.TimestampFieldType, Required: true},
		},
		TableCVMInformeMensal: {
			{Name: "cnpj", Type: bigquery.StringFieldType, Required: true},
			{Name: "ticker", Type: bigquery.StringFieldType},
			{Name: "reference_month", Type: bigquery.DateFieldType, Required: true},
			{Name: "name", Type: bigquery.StringFieldType},
			{Name: "isin", Type: bigquery.StringFieldType},
			{Name: "segment", Type: bigquery.StringFieldType},
			{Name: "mandate", Type: bigquery.StringFieldType},
			{Name: "administrator", Type: bigquery.StringFieldType},
			{Name: "listed", Type: bigquery.BooleanFieldType},
			{Name: "num_investors", Type: bigquery.IntegerFieldType},
			{Name: "assets_total", Type: bigquery.FloatFieldType},
			{Name: "equity_total", Type: bigquery.FloatFieldType},
			{Name: "shares_outstanding", Type: bigquery.FloatFieldType},
			{Name: "nav_per_share", Type: bigquery.FloatFieldType},
			{Name: "dividend_yield_month", Type: bigquery.FloatFieldType},
			{Name: "amortization_month", Type: bigquery.FloatFieldType},
			{Name: "vacancy_physical", Type: bigquery.FloatFieldType},
			{Name: "vacancy_financial", Type: bigquery.FloatFieldType},
			{Name: "real_estate_total", Type: bigquery.FloatFieldType},
			{Name: "financial_asset_total", Type: bigquery.FloatFieldType},
			{Name: "payload", Type: bigquery.JSONFieldType},
			{Name: "ingested_at", Type: bigquery.TimestampFieldType, Required: true},
		},
	}
}
