package cvm_test

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
	"github.com/addodelgrossi/reitbrazil-sync/internal/sources/cvm"
)

const informeGeralHeader = "Tipo_Fundo_Classe;CNPJ_Fundo_Classe;Data_Referencia;Versao;Data_Entrega;Nome_Fundo_Classe;Data_Funcionamento;Publico_Alvo;Codigo_ISIN;Quantidade_Cotas_Emitidas;Fundo_Exclusivo;Cotistas_Vinculo_Familiar;Mandato;Segmento_Atuacao;Tipo_Gestao;Prazo_Duracao;Data_Prazo_Duracao;Encerramento_Exercicio_Social;Mercado_Negociacao_Bolsa;Mercado_Negociacao_MBO;Mercado_Negociacao_MB\n"

// Each row needs at least 21 columns because listedBolsa is at index 19.
const informeGeralRows = "" +
	"Classe;11.728.688/0001-47;2025-01-01;1;2025-02-14;PATRIA LOG FII;2010-05-03;INVESTIDORES EM GERAL;BRXPLGCTF005;33787575;N;N;Renda;Logística;Ativa;Indeterminado;;31/12;S;N;N\n" +
	"Classe;97.521.225/0001-25;2025-01-01;1;2025-02-14;CSHG LOGISTICA FII;2011-02-20;INVESTIDORES EM GERAL;BRHGLGCTF004;40000000;N;N;Renda;Logística;Ativa;Indeterminado;;31/12;S;N;N\n" +
	"Classe;14.217.108/0001-45;2025-01-01;1;2025-02-14;FII INDUSTRIAL DO BRASIL RESP LTDA;2013-04-05;INVESTIDORES EM GERAL;;0;S;N;Renda;Logística;Ativa;Indeterminado;;31/12;N;N;N\n" +
	"Classe;01.356.517/0001-80;2025-01-01;1;2025-02-14;LAGRA FII;2010-01-15;INVESTIDORES EM GERAL;BR0H6PCTF001;1000000;N;N;Renda;Multicategoria;Ativa;Indeterminado;;31/12;N;N;N\n"

func TestParseInformeGeral_ExtractsBolsaAndTicker(t *testing.T) {
	buf := buildInformeGeralZip(t, "inf_mensal_fii_geral_2025.csv", informeGeralHeader+informeGeralRows)

	var rows []cvm.InformeGeralRow
	var errs []error
	for r, err := range cvm.ParseInformeGeral(t.Context(), buf) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		rows = append(rows, r)
	}
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	// XPLG — on B3, ticker derivable from ISIN.
	if rows[0].CNPJ != "11728688000147" {
		t.Errorf("row 0 cnpj = %q", rows[0].CNPJ)
	}
	if rows[0].Ticker != model.Ticker("XPLG11") {
		t.Errorf("row 0 ticker = %q, want XPLG11", rows[0].Ticker)
	}
	if !rows[0].ListedOnBolsa {
		t.Errorf("row 0 should be listed on B3")
	}
	if rows[0].Segment != "Logística" {
		t.Errorf("row 0 segment = %q", rows[0].Segment)
	}

	// FII INDUSTRIAL — empty ISIN, not on B3, no ticker.
	if rows[2].Ticker != "" {
		t.Errorf("row 2 should have no ticker, got %q", rows[2].Ticker)
	}
	if rows[2].ListedOnBolsa {
		t.Errorf("row 2 should NOT be listed on B3")
	}

	// LAGRA — non-standard ISIN (BR0H6PCTF001), 4-char block has digits so ticker extraction fails.
	if rows[3].Ticker != "" {
		t.Errorf("row 3 non-standard ISIN should yield no ticker, got %q", rows[3].Ticker)
	}
}

func TestParseInformeGeral_MalformedRowTolerated(t *testing.T) {
	body := informeGeralHeader +
		"Classe;11.728.688/0001-47;2025-01-01;1;2025-02-14;PATRIA LOG FII;2010-05-03;INVESTIDORES EM GERAL;BRXPLGCTF005;33787575;N;N;Renda;Logística;Ativa;Indeterminado;;31/12;S;N;N\n" +
		"Classe;22.222.222/0001-92;NOT-A-DATE;1;2025-02-14;BAD DATE FUNDO;2011-02-20;INVESTIDORES EM GERAL;BRXYZWCTF001;1000;N;N;Renda;Logística;Ativa;Indeterminado;;31/12;S;N;N\n" +
		"Classe;97.521.225/0001-25;2025-01-01;1;2025-02-14;CSHG LOGISTICA FII;2011-02-20;INVESTIDORES EM GERAL;BRHGLGCTF004;40000000;N;N;Renda;Logística;Ativa;Indeterminado;;31/12;S;N;N\n"
	zipBytes := buildInformeGeralZip(t, "inf_mensal_fii_geral_2025.csv", body)

	var good []cvm.InformeGeralRow
	var errs []error
	for r, err := range cvm.ParseInformeGeral(t.Context(), zipBytes) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		good = append(good, r)
	}
	if len(good) != 2 {
		t.Fatalf("expected 2 good rows, got %d", len(good))
	}
	if len(errs) != 1 {
		t.Fatalf("expected 1 row error, got %d", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "reference date") {
		t.Errorf("error should mention reference date: %v", errs[0])
	}
}

func TestParseInformeGeral_IgnoresNonGeralCSVs(t *testing.T) {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	mustAddFile(t, w, "inf_mensal_fii_ativo_passivo_2025.csv", "should;be;ignored\n")
	mustAddFile(t, w, "inf_mensal_fii_complemento_2025.csv", "also;ignored\n")
	mustAddFile(t, w, "inf_mensal_fii_geral_2025.csv", informeGeralHeader+
		"Classe;11.728.688/0001-47;2025-01-01;1;2025-02-14;PATRIA LOG FII;2010-05-03;INVESTIDORES EM GERAL;BRXPLGCTF005;33787575;N;N;Renda;Logística;Ativa;Indeterminado;;31/12;S;N;N\n")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	n := 0
	for _, err := range cvm.ParseInformeGeral(t.Context(), buf.Bytes()) {
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		n++
	}
	if n != 1 {
		t.Errorf("expected 1 row from geral only, got %d", n)
	}
}

func buildInformeGeralZip(t *testing.T, name, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	mustAddFile(t, w, name, body)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func mustAddFile(t *testing.T, w *zip.Writer, name, body string) {
	t.Helper()
	f, err := w.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
}
