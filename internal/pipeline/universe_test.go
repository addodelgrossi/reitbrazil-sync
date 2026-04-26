package pipeline

import (
	"archive/zip"
	"bytes"
	"context"
	"iter"
	"testing"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

func TestBuildFIIUniverse_PreservesCVMMetadata(t *testing.T) {
	d := Deps{
		Brapi: fakeBrapiSource{
			funds: []model.Fund{
				{Ticker: "XPLG11", Name: "XPLG11", Listed: true},
				{Ticker: "BOVA11", Name: "BOVA ETF", Listed: true},
			},
		},
		CVM: fakeCVMDownloader{zip: buildCVMUniverseZip(t)},
	}

	funds, stats, err := BuildFIIUniverse(t.Context(), d, 2025)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Intersection != 1 || stats.BrapiDropped != 1 {
		t.Fatalf("stats: %+v", stats)
	}
	if len(funds) != 1 {
		t.Fatalf("funds: %+v", funds)
	}
	got := funds[0]
	if got.Ticker != "XPLG11" || got.CNPJ != "11728688000147" || got.ISIN != "BRXPLGCTF005" {
		t.Fatalf("fund metadata: %+v", got)
	}
	if got.Name != "PATRIA LOG FII" || got.Segment != "logistic" || got.Mandate != "brick" {
		t.Fatalf("fund classification: %+v", got)
	}
}

func TestBuildFIIUniverse_FallsBackToCVMWhenBrapiListIsEmpty(t *testing.T) {
	d := Deps{
		Brapi: fakeBrapiSource{},
		CVM:   fakeCVMDownloader{zip: buildCVMUniverseZip(t)},
	}

	funds, stats, err := BuildFIIUniverse(t.Context(), d, 2025)
	if err != nil {
		t.Fatal(err)
	}
	if !stats.FallbackToCVM || stats.BrapiCount != 0 || stats.CVMB3WithTicker != 2 || stats.Intersection != 2 {
		t.Fatalf("stats: %+v", stats)
	}
	if len(funds) != 2 {
		t.Fatalf("funds: %+v", funds)
	}
	if funds[0].Ticker != "HGLG11" || funds[1].Ticker != "XPLG11" {
		t.Fatalf("sorted CVM fallback funds: %+v", funds)
	}
}

func TestRunDaily_DryRunDoesNotRequireBQOrPublisher(t *testing.T) {
	d := Deps{
		Brapi: fakeBrapiSource{
			funds: []model.Fund{{Ticker: "XPLG11", Name: "XPLG11", Listed: true}},
		},
		CVM: fakeCVMDownloader{zip: buildCVMUniverseZip(t)},
	}

	report, err := RunDaily(t.Context(), d, DailyOptions{DryRun: true, OutDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if report.Mode != "daily" || len(report.Stages) != 1 || report.Stages[0].Stage != "discover" {
		t.Fatalf("report: %+v", report)
	}
}

type fakeBrapiSource struct {
	funds []model.Fund
}

func (f fakeBrapiSource) FetchList(context.Context) iter.Seq2[model.Fund, error] {
	return func(yield func(model.Fund, error) bool) {
		for _, fund := range f.funds {
			if !yield(fund, nil) {
				return
			}
		}
	}
}

func (fakeBrapiSource) FetchHistory(context.Context, model.Ticker, time.Time, time.Time) iter.Seq2[model.PriceBar, error] {
	return func(func(model.PriceBar, error) bool) {}
}

func (fakeBrapiSource) FetchDividends(context.Context, model.Ticker) iter.Seq2[model.Dividend, error] {
	return func(func(model.Dividend, error) bool) {}
}

func (fakeBrapiSource) FetchFundamentals(context.Context, model.Ticker) (model.Fundamentals, error) {
	return model.Fundamentals{}, nil
}

type fakeCVMDownloader struct {
	zip []byte
}

func (f fakeCVMDownloader) FetchYear(context.Context, int) ([]byte, error) {
	return f.zip, nil
}

func buildCVMUniverseZip(t *testing.T) []byte {
	t.Helper()
	body := "Tipo_Fundo_Classe;CNPJ_Fundo_Classe;Data_Referencia;Nome_Fundo_Classe;Codigo_ISIN;Mandato;Segmento_Atuacao;Mercado_Negociacao_Bolsa;Nome_Administrador\n" +
		"Classe;11.728.688/0001-47;2025-12-31;PATRIA LOG FII;BRXPLGCTF005;Renda;Logística;S;Administrador X\n" +
		"Classe;97.521.225/0001-25;2025-12-31;CSHG LOG FII;BRHGLGCTF004;Renda;Logística;S;Administrador H\n"
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create("inf_mensal_fii_geral_2025.csv")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
