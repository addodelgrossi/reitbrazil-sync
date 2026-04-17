package cvm_test

import (
	"archive/zip"
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/cvm"
)

const sampleCSV = "CNPJ_Fundo;Data_Referencia;Codigo_B3;Numero_Cotistas;Patrimonio_Liquido;Valor_Patrimonial_Cotas;Percentual_Imoveis_Ocupados_Fisicamente;Percentual_Imoveis_Ocupados_Financeiramente\n" +
	"11.728.688/0001-47;2026-02-28;XPLG11;120000;5000000000,00;98,50;0,97;0,95\n" +
	"97.521.225/0001-25;2026-02-28;HGLG11;180000;8200000000,00;152,30;0,99;0,99\n"

func buildZip(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, body := range files {
		f, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestParse_YieldsRowsFromGeralCSV(t *testing.T) {
	zipBytes := buildZip(t, map[string]string{
		"inf_mensal_fii_geral_2026.csv":          sampleCSV,
		"inf_mensal_fii_ativo_passivo_2026.csv":  "should;be;ignored",
		"README.txt":                             "also ignored",
	})

	var rows []model.CVMInformeMensal
	var errs []error
	for r, err := range cvm.Parse(t.Context(), zipBytes) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		rows = append(rows, r)
	}
	if len(errs) > 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].CNPJ != "11728688000147" {
		t.Fatalf("cnpj: %q", rows[0].CNPJ)
	}
	if rows[0].Ticker != "XPLG11" {
		t.Fatalf("ticker: %q", rows[0].Ticker)
	}
	if rows[0].NumInvestors != 120000 {
		t.Fatalf("investors: %d", rows[0].NumInvestors)
	}
	if rows[0].EquityTotal != 5_000_000_000 {
		t.Fatalf("equity: %v", rows[0].EquityTotal)
	}
	if rows[0].NAVPerShare != 98.50 {
		t.Fatalf("nav: %v", rows[0].NAVPerShare)
	}
	if rows[1].Ticker != "HGLG11" {
		t.Fatalf("second ticker: %q", rows[1].Ticker)
	}
}

func TestParseCSV_Latin1(t *testing.T) {
	// Build a Latin-1 CSV (é = 0xE9) to exercise the transcoder.
	header := []byte("CNPJ_Fundo;Data_Referencia;Codigo_B3;Numero_Cotistas;Patrimonio_L\xEDquido;Valor_Patrimonial_Cotas;Percentual_Im\xF3veis_Ocupados_Fisicamente;Percentual_Im\xF3veis_Ocupados_Financeiramente\n")
	row := []byte("11.728.688/0001-47;2026-01-31;XPLG11;100000;4800000000,00;97,00;0,98;0,98\n")
	body := append(header, row...)

	// Quick sanity: it should not be valid UTF-8 before transcoding.
	if !bytes.ContainsAny(body, "\xE9\xF3") {
		t.Fatal("test CSV missing Latin-1 bytes")
	}

	var rows []model.CVMInformeMensal
	for r, err := range cvm.ParseCSV(t.Context(), bytes.NewReader(body)) {
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].CNPJ != "11728688000147" {
		t.Fatalf("cnpj: %q", rows[0].CNPJ)
	}
}

func TestDownloader_FetchYear(t *testing.T) {
	zipBytes := buildZip(t, map[string]string{
		"inf_mensal_fii_geral_2026.csv": sampleCSV,
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "inf_mensal_fii_2026.zip") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(zipBytes)
	}))
	defer srv.Close()

	d := cvm.NewDownloader(cvm.DownloaderOptions{BaseURL: srv.URL})
	got, err := d.FetchYear(t.Context(), 2026)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, zipBytes) {
		t.Fatalf("downloaded bytes differ (got %d vs want %d)", len(got), len(zipBytes))
	}
}

func TestDownloader_RejectsOldYear(t *testing.T) {
	d := cvm.NewDownloader(cvm.DownloaderOptions{})
	if _, err := d.FetchYear(t.Context(), 2015); err == nil {
		t.Fatal("expected error for year < 2016")
	}
}

// io.EOF is exposed from io; reference it so the import is used under
// all build configurations.
var _ = io.EOF
