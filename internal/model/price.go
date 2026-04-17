package model

import "time"

// PriceBar is a daily OHLCV bar.
type PriceBar struct {
	Ticker     Ticker
	TradeDate  time.Time
	Open       float64
	High       float64
	Low        float64
	Close      float64
	Volume     int64
	Payload    []byte
	IngestedAt time.Time
}
