package model

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrInvalidTicker is returned when a ticker does not match the B3 REIT pattern.
var ErrInvalidTicker = errors.New("invalid ticker")

// Ticker is a validated B3 REIT ticker (four uppercase letters + two digits),
// e.g. XPLG11. The pattern matches what the MCP server validates in
// github.com/addodelgrossi/reitbrazil/internal/domain.ParseTicker.
type Ticker string

var tickerPattern = regexp.MustCompile(`^[A-Z]{4}\d{2}$`)

// ParseTicker trims, uppercases, and validates raw.
func ParseTicker(raw string) (Ticker, error) {
	t := strings.ToUpper(strings.TrimSpace(raw))
	if !tickerPattern.MatchString(t) {
		return "", fmt.Errorf("%w: %q", ErrInvalidTicker, raw)
	}
	return Ticker(t), nil
}

// MustParseTicker panics on invalid input; use in tests and seeds only.
func MustParseTicker(raw string) Ticker {
	t, err := ParseTicker(raw)
	if err != nil {
		panic(err)
	}
	return t
}

// String implements fmt.Stringer.
func (t Ticker) String() string { return string(t) }
