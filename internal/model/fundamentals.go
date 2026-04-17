package model

import "time"

// Fundamentals is a periodic snapshot of a fund's fundamentals.
type Fundamentals struct {
	Ticker           Ticker
	AsOf             time.Time
	NAVPerShare      float64
	PVP              float64
	AssetsTotal      float64
	EquityTotal      float64
	NumInvestors     int64
	Liquidity90d     float64
	VacancyPhysical  float64
	VacancyFinancial float64
	OccupancyRate    float64
	Payload          []byte
	IngestedAt       time.Time
}

// CVMInformeMensal is the monthly informe row parsed from the CVM open-data portal.
type CVMInformeMensal struct {
	CNPJ             string
	Ticker           Ticker
	ReferenceMonth   time.Time
	NumInvestors     int64
	EquityTotal      float64
	NAVPerShare      float64
	VacancyPhysical  float64
	VacancyFinancial float64
	Payload          []byte
	IngestedAt       time.Time
}
