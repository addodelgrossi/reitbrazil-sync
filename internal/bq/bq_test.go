package bq_test

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/addodelgrossi/reitbrazil-sync/internal/bq"
)

// embeddedFS is re-exposed for the test so we can inspect what was packed
// into the binary.
var embeddedFS = bq.EmbeddedSQLForTest()

func TestEmbeddedSQLFiles_AllPresent(t *testing.T) {
	want := []string{
		"01_create_raw_tables.sql",
		"02_create_canon_tables.sql",
		"10_transform_funds.sql",
		"11_transform_prices.sql",
		"12_transform_dividends.sql",
		"13_transform_fundamentals.sql",
		"20_materialize_snapshots.sql",
	}
	for _, name := range want {
		body, err := fs.ReadFile(embeddedFS, name)
		if err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
		if len(body) == 0 {
			t.Fatalf("%s is empty", name)
		}
		if !strings.Contains(string(body), "${project}") &&
			!strings.Contains(string(body), "CREATE TABLE") {
			t.Fatalf("%s looks wrong: %q", name, firstLine(string(body)))
		}
	}
}

func TestRawSchemas_CoversEveryRawTable(t *testing.T) {
	s := bq.RawSchemas()
	for _, name := range []string{
		bq.TableBrapiFundList, bq.TableBrapiQuote, bq.TableBrapiDividends,
		bq.TableBrapiFundamentals, bq.TableCVMInformeMensal,
	} {
		if _, ok := s[name]; !ok {
			t.Fatalf("missing schema: %s", name)
		}
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
