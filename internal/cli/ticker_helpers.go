package cli

import "github.com/addodelgrossi/reitbrazil-sync/internal/model"

func parseTickerModel(s string) (model.Ticker, error) { return model.ParseTicker(s) }
