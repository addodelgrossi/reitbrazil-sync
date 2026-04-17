package model

import "time"

// Fund is the canonical fund registry row.
type Fund struct {
	Ticker        Ticker
	CNPJ          string
	Name          string
	Segment       string
	Mandate       string
	Manager       string
	Administrator string
	IPODate       *time.Time
	Listed        bool
	Payload       []byte
	IngestedAt    time.Time
}
