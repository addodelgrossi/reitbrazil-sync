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

const sampleGeralCSV = "Tipo_Fundo_Classe;CNPJ_Fundo_Classe;Data_Referencia;Nome_Fundo_Classe;Codigo_ISIN;Mandato;Segmento_Atuacao;Mercado_Negociacao_Bolsa;Nome_Administrador\n" +
	"Classe;11.728.688/0001-47;2026-02-28;PATRIA LOG FII;BRXPLGCTF005;Renda;Logística;S;Administrador X\n" +
	"Classe;97.521.225/0001-25;2026-02-28;CSHG LOG FII;BRHGLGCTF004;Renda;Logística;S;Administrador H\n"

const sampleComplementoCSV = "CNPJ_Fundo_Classe;Data_Referencia;Total_Numero_Cotistas;Valor_Ativo;Patrimonio_Liquido;Cotas_Emitidas;Valor_Patrimonial_Cotas;Percentual_Dividend_Yield_Mes;Percentual_Amortizacao_Cotas_Mes\n" +
	"11.728.688/0001-47;2026-02-28;120000;5100000000,00;5000000000,00;50761421;98,50;0,76;0,00\n" +
	"97.521.225/0001-25;2026-02-28;180000;8300000000,00;8200000000,00;53841000;152,30;0,69;0,00\n"

const sampleAtivoPassivoCSV = "CNPJ_Fundo_Classe;Data_Referencia;Total_Investido;Direitos_Bens_Imoveis\n" +
	"11.728.688/0001-47;2026-02-28;4900000000,00;4500000000,00\n" +
	"97.521.225/0001-25;2026-02-28;8000000000,00;7600000000,00\n"

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
		"inf_mensal_fii_geral_2026.csv":         sampleGeralCSV,
		"inf_mensal_fii_complemento_2026.csv":   sampleComplementoCSV,
		"inf_mensal_fii_ativo_passivo_2026.csv": sampleAtivoPassivoCSV,
		"README.txt":                            "also ignored",
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
	if rows[0].NumInvestors == nil || *rows[0].NumInvestors != 120000 {
		t.Fatalf("investors: %v", rows[0].NumInvestors)
	}
	if rows[0].EquityTotal == nil || *rows[0].EquityTotal != 5_000_000_000 {
		t.Fatalf("equity: %v", rows[0].EquityTotal)
	}
	if rows[0].NAVPerShare == nil || *rows[0].NAVPerShare != 98.50 {
		t.Fatalf("nav: %v", rows[0].NAVPerShare)
	}
	if rows[0].VacancyPhysical != nil {
		t.Fatalf("monthly parser should not invent vacancy: %v", rows[0].VacancyPhysical)
	}
	if rows[1].Ticker != "HGLG11" {
		t.Fatalf("second ticker: %q", rows[1].Ticker)
	}
}

func TestParseCSV_Latin1(t *testing.T) {
	// Build a Latin-1 CSV (é = 0xE9) to exercise the transcoder.
	header := []byte("CNPJ_Fundo_Classe;Data_Referencia;Total_Numero_Cotistas;Patrimonio_L\xEDquido;Valor_Patrimonial_Cotas\n")
	row := []byte("11.728.688/0001-47;2026-01-31;100000;4800000000,00;97,00\n")
	body := make([]byte, 0, len(header)+len(row))
	body = append(body, header...)
	body = append(body, row...)

	// Quick sanity: it should not be valid UTF-8 before transcoding.
	if !bytes.ContainsAny(body, "\xED") {
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
		"inf_mensal_fii_geral_2026.csv": sampleGeralCSV,
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
