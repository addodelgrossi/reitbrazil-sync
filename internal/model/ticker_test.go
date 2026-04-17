package model_test

import (
	"errors"
	"testing"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

func TestParseTicker(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want model.Ticker
		err  bool
	}{
		{"XPLG11", "XPLG11", false},
		{" hglg11 ", "HGLG11", false},
		{"abc123", "", true},
		{"AAA111", "", true},
		{"", "", true},
		{"XPLG 11", "", true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := model.ParseTicker(tc.in)
			if tc.err {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				if !errors.Is(err, model.ErrInvalidTicker) {
					t.Fatalf("expected ErrInvalidTicker, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("parse %q: got %q want %q", tc.in, got, tc.want)
			}
		})
	}
}
