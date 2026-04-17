package model

import "time"

// DividendKind classifies a cash distribution event.
type DividendKind string

const (
	DividendKindDividend     DividendKind = "dividend"
	DividendKindAmortization DividendKind = "amortization"
	DividendKindRights       DividendKind = "rights"
)

// Dividend is a single cash-distribution event.
type Dividend struct {
	Ticker         Ticker
	AnnounceDate   *time.Time
	ExDate         time.Time
	RecordDate     *time.Time
	PaymentDate    *time.Time
	AmountPerShare float64
	Kind           DividendKind
	Source         string
	Payload        []byte
	IngestedAt     time.Time
}
