package cvm

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

// Parse opens the in-memory ZIP and yields one InformeMensal per data
// row across every inf_mensal_fii_geral_*.csv file it contains. Other
// CSVs in the ZIP (ativo_passivo, complemento) are ignored here;
// complements could be enriched in a later iteration.
func Parse(ctx context.Context, zipBytes []byte) iter.Seq2[model.CVMInformeMensal, error] {
	return func(yield func(model.CVMInformeMensal, error) bool) {
		r, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
		if err != nil {
			yield(model.CVMInformeMensal{}, fmt.Errorf("open zip: %w", err))
			return
		}

		for _, f := range r.File {
			name := strings.ToLower(f.Name)
			if !strings.Contains(name, "inf_mensal_fii") || !strings.HasSuffix(name, ".csv") {
				continue
			}
			if strings.Contains(name, "ativo_passivo") || strings.Contains(name, "complemento") {
				continue
			}
			if err := ctx.Err(); err != nil {
				yield(model.CVMInformeMensal{}, err)
				return
			}

			rc, err := f.Open()
			if err != nil {
				yield(model.CVMInformeMensal{}, fmt.Errorf("open %s: %w", f.Name, err))
				return
			}
			if !parseCSV(ctx, rc, yield) {
				rc.Close()
				return
			}
			rc.Close()
		}
	}
}

// ParseCSV is a thin wrapper that parses a single CSV stream. Exposed
// for tests and for adapters that already unpacked the ZIP.
func ParseCSV(ctx context.Context, r io.Reader) iter.Seq2[model.CVMInformeMensal, error] {
	return func(yield func(model.CVMInformeMensal, error) bool) {
		parseCSV(ctx, r, yield)
	}
}

func parseCSV(ctx context.Context, r io.Reader, yield func(model.CVMInformeMensal, error) bool) bool {
	reader := csv.NewReader(newUTF8Reader(r))
	reader.Comma = ';'
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		return yield(model.CVMInformeMensal{}, fmt.Errorf("read header: %w", err))
	}
	cols := indexColumns(header)
	ingested := time.Now().UTC()

	for {
		if err := ctx.Err(); err != nil {
			return yield(model.CVMInformeMensal{}, err)
		}
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return true
		}
		if err != nil {
			return yield(model.CVMInformeMensal{}, fmt.Errorf("read row: %w", err))
		}
		rec, err := mapRow(cols, row, ingested)
		if err != nil {
			// Tolerate individual bad rows — emit error but keep going.
			if !yield(model.CVMInformeMensal{}, err) {
				return false
			}
			continue
		}
		if !yield(rec, nil) {
			return false
		}
	}
}

type colIndex struct {
	cnpj             int
	reference        int
	ticker           int
	numInvestors     int
	equityTotal      int
	navPerShare      int
	vacancyPhysical  int
	vacancyFinancial int
}

func indexColumns(header []string) colIndex {
	idx := colIndex{
		cnpj: -1, reference: -1, ticker: -1, numInvestors: -1,
		equityTotal: -1, navPerShare: -1, vacancyPhysical: -1, vacancyFinancial: -1,
	}
	for i, raw := range header {
		key := normalizeHeader(raw)
		switch key {
		case "cnpj_fundo", "cnpj_fundo_classe", "cnpj":
			idx.cnpj = i
		case "data_referencia", "data_competencia":
			idx.reference = i
		case "codigo_b3", "codigo_negociacao", "ticker":
			idx.ticker = i
		case "numero_cotistas", "num_cotistas":
			idx.numInvestors = i
		case "patrimonio_liquido", "patrimonio_total":
			idx.equityTotal = i
		case "valor_patrimonial_cotas", "valor_patrimonial_cota":
			idx.navPerShare = i
		case "percentual_imoveis_ocupados_fisicamente", "percentual_ocupacao_fisica":
			idx.vacancyPhysical = i
		case "percentual_imoveis_ocupados_financeiramente", "percentual_ocupacao_financeira":
			idx.vacancyFinancial = i
		}
	}
	return idx
}

func mapRow(idx colIndex, row []string, ingested time.Time) (model.CVMInformeMensal, error) {
	get := func(i int) string {
		if i < 0 || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	if idx.cnpj < 0 || idx.reference < 0 {
		return model.CVMInformeMensal{}, errors.New("cvm: missing required columns (CNPJ_Fundo, Data_Referencia)")
	}

	cnpj := normalizeCNPJ(get(idx.cnpj))
	if cnpj == "" {
		return model.CVMInformeMensal{}, errors.New("cvm: empty cnpj")
	}
	ref, err := parseCVMDate(get(idx.reference))
	if err != nil {
		return model.CVMInformeMensal{}, fmt.Errorf("cvm: reference date: %w", err)
	}

	ticker := model.Ticker("")
	if raw := get(idx.ticker); raw != "" {
		if t, err := model.ParseTicker(raw); err == nil {
			ticker = t
		}
	}

	rec := model.CVMInformeMensal{
		CNPJ:             cnpj,
		Ticker:           ticker,
		ReferenceMonth:   ref,
		NumInvestors:     parseInt(get(idx.numInvestors)),
		EquityTotal:      parseFloat(get(idx.equityTotal)),
		NAVPerShare:      parseFloat(get(idx.navPerShare)),
		VacancyPhysical:  parseFloat(get(idx.vacancyPhysical)),
		VacancyFinancial: parseFloat(get(idx.vacancyFinancial)),
		IngestedAt:       ingested,
	}

	payload := map[string]string{}
	for i, col := range row {
		if i < len(row) {
			payload[fmt.Sprintf("col_%d", i)] = col
		}
	}
	_ = payload // reserved for future enrichment; keep footprint small for now

	p, _ := json.Marshal(struct {
		CNPJ      string `json:"cnpj"`
		Reference string `json:"reference_month"`
		Ticker    string `json:"ticker,omitempty"`
	}{cnpj, ref.Format("2006-01-02"), string(ticker)})
	rec.Payload = p

	return rec, nil
}

func normalizeHeader(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	s = stripAccents(s)
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, " ", "_")
	// Remove BOM
	s = strings.TrimPrefix(s, "\ufeff")
	return s
}

func parseCVMDate(raw string) (time.Time, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return time.Time{}, errors.New("empty date")
	}
	layouts := []string{"2006-01-02", "02/01/2006", "01/2006", "2006-01"}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unknown date %q", s)
}

func normalizeCNPJ(raw string) string {
	var b strings.Builder
	for _, r := range raw {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func parseInt(raw string) int64 {
	s := strings.ReplaceAll(raw, ".", "")
	s = strings.ReplaceAll(s, ",", ".")
	if s == "" {
		return 0
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f)
	}
	return 0
}

func parseFloat(raw string) float64 {
	s := strings.ReplaceAll(raw, ".", "")
	s = strings.ReplaceAll(s, ",", ".")
	if s == "" {
		return 0
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return 0
}

// stripAccents does a minimal ASCII fold for the handful of Portuguese
// headers we care about. Not a full Unicode NFD normalizer, but enough
// to canonicalise "patrimônio" → "patrimonio".
func stripAccents(s string) string {
	repl := strings.NewReplacer(
		"á", "a", "à", "a", "â", "a", "ã", "a", "ä", "a",
		"é", "e", "è", "e", "ê", "e", "ë", "e",
		"í", "i", "ì", "i", "î", "i", "ï", "i",
		"ó", "o", "ò", "o", "ô", "o", "õ", "o", "ö", "o",
		"ú", "u", "ù", "u", "û", "u", "ü", "u",
		"ç", "c", "ñ", "n",
	)
	return repl.Replace(s)
}
