package cvm

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"iter"
	"regexp"
	"strings"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

// InformeGeralRow is the subset of the new Res 175 geral CSV we use to
// describe the FII universe. The geral CSV (inf_mensal_fii_geral_YYYY.csv)
// carries the fund identity + B3 listing flags; balance-sheet details
// live in ativo_passivo.
type InformeGeralRow struct {
	CNPJ           string
	Name           string
	ReferenceMonth time.Time
	ISIN           string
	Ticker         model.Ticker // derived from ISIN when possible
	Mandate        string       // Mandato: Renda, Titulos_Valores_Mobiliarios, ...
	Segment        string       // Segmento_Atuacao: Logística, Shopping, ...
	ListedOnBolsa  bool         // Mercado_Negociacao_Bolsa == "S"
	IngestedAt     time.Time
}

// ParseInformeGeral reads inf_mensal_fii_geral_YYYY.csv files out of a
// full informe ZIP. Other CSVs in the ZIP (ativo_passivo, complemento)
// are ignored. Row-level parse errors are yielded as non-fatal errors.
func ParseInformeGeral(ctx context.Context, zipBytes []byte) iter.Seq2[InformeGeralRow, error] {
	return func(yield func(InformeGeralRow, error) bool) {
		r, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
		if err != nil {
			yield(InformeGeralRow{}, fmt.Errorf("open zip: %w", err))
			return
		}
		for _, f := range r.File {
			name := strings.ToLower(f.Name)
			if !strings.Contains(name, "inf_mensal_fii_geral") || !strings.HasSuffix(name, ".csv") {
				continue
			}
			if err := ctx.Err(); err != nil {
				yield(InformeGeralRow{}, err)
				return
			}
			rc, err := f.Open()
			if err != nil {
				yield(InformeGeralRow{}, fmt.Errorf("open %s: %w", f.Name, err))
				return
			}
			if !parseInformeGeralCSV(ctx, rc, yield) {
				_ = rc.Close()
				return
			}
			if err := rc.Close(); err != nil {
				yield(InformeGeralRow{}, fmt.Errorf("close %s: %w", f.Name, err))
				return
			}
		}
	}
}

// ParseInformeGeralCSV parses an already-opened CSV stream. Exposed
// for tests and callers that unpack the ZIP themselves.
func ParseInformeGeralCSV(ctx context.Context, r io.Reader) iter.Seq2[InformeGeralRow, error] {
	return func(yield func(InformeGeralRow, error) bool) {
		parseInformeGeralCSV(ctx, r, yield)
	}
}

func parseInformeGeralCSV(ctx context.Context, r io.Reader, yield func(InformeGeralRow, error) bool) bool {
	reader := csv.NewReader(newUTF8Reader(r))
	reader.Comma = ';'
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		return yield(InformeGeralRow{}, fmt.Errorf("read header: %w", err))
	}
	cols := indexInformeGeralColumns(header)
	if cols.cnpj < 0 || cols.reference < 0 {
		return yield(InformeGeralRow{}, errors.New("cvm informe geral: missing CNPJ_Fundo_Classe / Data_Referencia"))
	}
	ingested := time.Now().UTC()

	for {
		if err := ctx.Err(); err != nil {
			return yield(InformeGeralRow{}, err)
		}
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return true
		}
		if err != nil {
			return yield(InformeGeralRow{}, fmt.Errorf("read row: %w", err))
		}
		rec, err := mapInformeGeralRow(cols, row, ingested)
		if err != nil {
			if !yield(InformeGeralRow{}, err) {
				return false
			}
			continue
		}
		if !yield(rec, nil) {
			return false
		}
	}
}

type informeGeralCols struct {
	cnpj        int
	reference   int
	name        int
	isin        int
	mandate     int
	segment     int
	listedBolsa int
}

func indexInformeGeralColumns(header []string) informeGeralCols {
	idx := informeGeralCols{cnpj: -1, reference: -1, name: -1, isin: -1, mandate: -1, segment: -1, listedBolsa: -1}
	for i, raw := range header {
		key := normalizeHeader(raw)
		switch key {
		case "cnpj_fundo_classe", "cnpj_fundo", "cnpj":
			idx.cnpj = i
		case "data_referencia", "data_competencia":
			idx.reference = i
		case "nome_fundo_classe", "nome_fundo", "denom_social":
			idx.name = i
		case "codigo_isin", "isin":
			idx.isin = i
		case "mandato":
			idx.mandate = i
		case "segmento_atuacao", "segmento":
			idx.segment = i
		case "mercado_negociacao_bolsa":
			idx.listedBolsa = i
		}
	}
	return idx
}

// isinTickerPattern matches FII cota ISINs of the form BR<4-char>CTF<digits>.
// The 4-char block is the B3 ticker prefix; combined with the fixed "11"
// suffix it yields the common FII cota ticker (e.g. BRXPLGCTF005 → XPLG11).
var isinTickerPattern = regexp.MustCompile(`^BR([A-Z]{4})CTF\d+$`)

func tickerFromISIN(isin string) model.Ticker {
	m := isinTickerPattern.FindStringSubmatch(strings.ToUpper(strings.TrimSpace(isin)))
	if m == nil {
		return ""
	}
	t, err := model.ParseTicker(m[1] + "11")
	if err != nil {
		return ""
	}
	return t
}

func mapInformeGeralRow(cols informeGeralCols, row []string, ingested time.Time) (InformeGeralRow, error) {
	get := func(i int) string {
		if i < 0 || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}
	cnpj := normalizeCNPJ(get(cols.cnpj))
	if cnpj == "" {
		return InformeGeralRow{}, errors.New("informe geral: empty cnpj")
	}
	ref, err := parseCVMDate(get(cols.reference))
	if err != nil {
		return InformeGeralRow{}, fmt.Errorf("informe geral %s: reference date: %w", cnpj, err)
	}
	isin := get(cols.isin)
	return InformeGeralRow{
		CNPJ:           cnpj,
		Name:           get(cols.name),
		ReferenceMonth: ref,
		ISIN:           isin,
		Ticker:         tickerFromISIN(isin),
		Mandate:        get(cols.mandate),
		Segment:        get(cols.segment),
		ListedOnBolsa:  strings.EqualFold(get(cols.listedBolsa), "S"),
		IngestedAt:     ingested,
	}, nil
}
