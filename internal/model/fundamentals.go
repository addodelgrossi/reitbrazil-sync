package model

import "time"

// Fundamentals is a periodic snapshot of a fund's fundamentals.
type Fundamentals struct {
	Ticker           Ticker
	AsOf             time.Time
	NAVPerShare      *float64
	PVP              *float64
	AssetsTotal      *float64
	EquityTotal      *float64
	NumInvestors     *int64
	Liquidity90d     *float64
	VacancyPhysical  *float64
	VacancyFinancial *float64
	OccupancyRate    *float64
	Payload          []byte
	IngestedAt       time.Time
}

// CVMInformeMensal is the monthly informe row parsed from the CVM open-data portal.
type CVMInformeMensal struct {
	CNPJ                string
	Ticker              Ticker
	ReferenceMonth      time.Time
	Name                string
	ISIN                string
	Segment             string
	Mandate             string
	Administrator       string
	Listed              *bool
	NumInvestors        *int64
	AssetsTotal         *float64
	EquityTotal         *float64
	SharesOutstanding   *float64
	NAVPerShare         *float64
	DividendYieldMonth  *float64
	AmortizationMonth   *float64
	VacancyPhysical     *float64
	VacancyFinancial    *float64
	RealEstateTotal     *float64
	FinancialAssetTotal *float64
	Payload             []byte
	IngestedAt          time.Time
}

// CVMPropertyVacancy is a quarterly property-level vacancy observation
// from the CVM trimestral imovel CSVs.
type CVMPropertyVacancy struct {
	CNPJ             string
	ReferenceQuarter time.Time
	PropertyName     string
	PropertyClass    string
	VacancyPhysical  *float64
	DelinquencyRate  *float64
	RevenueShare     *float64
	LeasedShare      *float64
	SoldShare        *float64
	Payload          []byte
	IngestedAt       time.Time
}
