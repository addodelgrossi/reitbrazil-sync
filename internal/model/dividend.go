package model

import "time"

// DistributionKind classifies a cash distribution event.
type DistributionKind string

const (
	DistributionKindDividend     DistributionKind = "dividend"
	DistributionKindAmortization DistributionKind = "amortization"
	DistributionKindRights       DistributionKind = "rights"
)

// Distribution is a single cash-distribution event.
type Distribution struct {
	Ticker         Ticker
	AnnounceDate   *time.Time
	ExDate         time.Time
	RecordDate     *time.Time
	PaymentDate    *time.Time
	AmountPerShare float64
	Kind           DistributionKind
	Source         string
	Payload        []byte
	IngestedAt     time.Time
}

// Dividend is kept as a compatibility alias for the SQLite table and
// existing MCP terminology. Internally, new code should prefer Distribution.
type Dividend = Distribution

// DividendKind is kept as a compatibility alias for existing callers.
type DividendKind = DistributionKind

const (
	DividendKindDividend     = DistributionKindDividend
	DividendKindAmortization = DistributionKindAmortization
	DividendKindRights       = DistributionKindRights
)
