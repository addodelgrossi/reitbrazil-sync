package bq

import (
	"time"

	"cloud.google.com/go/civil"
)

// rawFundRow mirrors raw.brapi_fund_list.
type rawFundRow struct {
	Ticker        string    `bigquery:"ticker"`
	CNPJ          string    `bigquery:"cnpj"`
	ISIN          string    `bigquery:"isin"`
	ShortName     string    `bigquery:"short_name"`
	LongName      string    `bigquery:"long_name"`
	Segment       string    `bigquery:"segment"`
	Mandate       string    `bigquery:"mandate"`
	Manager       string    `bigquery:"manager"`
	Administrator string    `bigquery:"administrator"`
	Listed        *bool     `bigquery:"listed"`
	Payload       string    `bigquery:"payload"`
	IngestedAt    time.Time `bigquery:"ingested_at"`
}

// rawQuoteRow mirrors raw.brapi_quote.
type rawQuoteRow struct {
	Ticker     string     `bigquery:"ticker"`
	TradeDate  civil.Date `bigquery:"trade_date"`
	Open       float64    `bigquery:"open"`
	High       float64    `bigquery:"high"`
	Low        float64    `bigquery:"low"`
	Close      float64    `bigquery:"close"`
	Volume     int64      `bigquery:"volume"`
	Payload    string     `bigquery:"payload"`
	IngestedAt time.Time  `bigquery:"ingested_at"`
}

// rawDividendRow mirrors raw.brapi_dividends.
type rawDividendRow struct {
	EventID      string      `bigquery:"event_id"`
	Ticker       string      `bigquery:"ticker"`
	ExDate       civil.Date  `bigquery:"ex_date"`
	AnnounceDate *civil.Date `bigquery:"announce_date"`
	RecordDate   *civil.Date `bigquery:"record_date"`
	PaymentDate  *civil.Date `bigquery:"payment_date"`
	Amount       float64     `bigquery:"amount"`
	Kind         string      `bigquery:"kind"`
	Source       string      `bigquery:"source"`
	Payload      string      `bigquery:"payload"`
	IngestedAt   time.Time   `bigquery:"ingested_at"`
}

// rawFundamentalsRow mirrors raw.brapi_fundamentals.
type rawFundamentalsRow struct {
	Ticker           string     `bigquery:"ticker"`
	AsOf             civil.Date `bigquery:"as_of"`
	NAVPerShare      *float64   `bigquery:"nav_per_share"`
	PVP              *float64   `bigquery:"pvp"`
	AssetsTotal      *float64   `bigquery:"assets_total"`
	EquityTotal      *float64   `bigquery:"equity_total"`
	NumInvestors     *int64     `bigquery:"num_investors"`
	Liquidity90d     *float64   `bigquery:"liquidity_90d"`
	VacancyPhysical  *float64   `bigquery:"vacancy_physical"`
	VacancyFinancial *float64   `bigquery:"vacancy_financial"`
	OccupancyRate    *float64   `bigquery:"occupancy_rate"`
	Payload          string     `bigquery:"payload"`
	IngestedAt       time.Time  `bigquery:"ingested_at"`
}

// rawCVMInformeRow mirrors raw.cvm_informe_mensal.
type rawCVMInformeRow struct {
	CNPJ                string     `bigquery:"cnpj"`
	Ticker              string     `bigquery:"ticker"`
	ReferenceMonth      civil.Date `bigquery:"reference_month"`
	Name                string     `bigquery:"name"`
	ISIN                string     `bigquery:"isin"`
	Segment             string     `bigquery:"segment"`
	Mandate             string     `bigquery:"mandate"`
	Administrator       string     `bigquery:"administrator"`
	Listed              *bool      `bigquery:"listed"`
	NumInvestors        *int64     `bigquery:"num_investors"`
	AssetsTotal         *float64   `bigquery:"assets_total"`
	EquityTotal         *float64   `bigquery:"equity_total"`
	SharesOutstanding   *float64   `bigquery:"shares_outstanding"`
	NAVPerShare         *float64   `bigquery:"nav_per_share"`
	DividendYieldMonth  *float64   `bigquery:"dividend_yield_month"`
	AmortizationMonth   *float64   `bigquery:"amortization_month"`
	VacancyPhysical     *float64   `bigquery:"vacancy_physical"`
	VacancyFinancial    *float64   `bigquery:"vacancy_financial"`
	RealEstateTotal     *float64   `bigquery:"real_estate_total"`
	FinancialAssetTotal *float64   `bigquery:"financial_asset_total"`
	Payload             string     `bigquery:"payload"`
	IngestedAt          time.Time  `bigquery:"ingested_at"`
}

// civilDate converts t (UTC date) into civil.Date.
func civilDate(t time.Time) civil.Date {
	return civil.DateOf(t.UTC())
}

// nullableCivilDate returns a *civil.Date for BigQuery NULL semantics.
func nullableCivilDate(t *time.Time) *civil.Date {
	if t == nil || t.IsZero() {
		return nil
	}
	d := civil.DateOf(t.UTC())
	return &d
}
