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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

// Parse opens the in-memory ZIP and yields one consolidated monthly
// informe per (CNPJ, reference_month). The CVM monthly package splits
// identity, balance-sheet, and investor metrics across geral,
// complemento, and ativo_passivo CSVs, so this parser merges all three.
func Parse(ctx context.Context, zipBytes []byte) iter.Seq2[model.CVMInformeMensal, error] {
	return func(yield func(model.CVMInformeMensal, error) bool) {
		r, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
		if err != nil {
			yield(model.CVMInformeMensal{}, fmt.Errorf("open zip: %w", err))
			return
		}

		acc := map[monthlyKey]*monthlyAccumulator{}
		ingested := time.Now().UTC()
		for _, f := range r.File {
			kind := monthlyKindFromName(f.Name)
			if kind == monthlyKindUnknown {
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
			ok := parseMonthlyCSV(ctx, rc, f.Name, kind, ingested, acc, func(err error) bool {
				return yield(model.CVMInformeMensal{}, err)
			})
			if err := rc.Close(); err != nil && ok {
				ok = yield(model.CVMInformeMensal{}, fmt.Errorf("close %s: %w", f.Name, err))
			}
			if !ok {
				return
			}
		}

		for _, k := range sortedMonthlyKeys(acc) {
			rec := acc[k].record
			payload, err := json.Marshal(monthlyPayload{Records: acc[k].records})
			if err != nil {
				yield(model.CVMInformeMensal{}, fmt.Errorf("payload %s: %w", k.cnpj, err))
				return
			}
			rec.Payload = payload
			if !yield(rec, nil) {
				return
			}
		}
	}
}

// ParseCSV parses a single monthly CSV stream. It is mainly used by
// tests and by callers that unpacked the ZIP themselves.
func ParseCSV(ctx context.Context, r io.Reader) iter.Seq2[model.CVMInformeMensal, error] {
	return func(yield func(model.CVMInformeMensal, error) bool) {
		acc := map[monthlyKey]*monthlyAccumulator{}
		ok := parseMonthlyCSV(ctx, r, "inline.csv", monthlyKindUnknown, time.Now().UTC(), acc, func(err error) bool {
			return yield(model.CVMInformeMensal{}, err)
		})
		if !ok {
			return
		}
		for _, k := range sortedMonthlyKeys(acc) {
			rec := acc[k].record
			payload, err := json.Marshal(monthlyPayload{Records: acc[k].records})
			if err != nil {
				yield(model.CVMInformeMensal{}, err)
				return
			}
			rec.Payload = payload
			if !yield(rec, nil) {
				return
			}
		}
	}
}

type monthlyKind string

const (
	monthlyKindUnknown      monthlyKind = ""
	monthlyKindGeral        monthlyKind = "geral"
	monthlyKindComplemento  monthlyKind = "complemento"
	monthlyKindAtivoPassivo monthlyKind = "ativo_passivo" // #nosec G101 -- CVM file kind, not a credential.
)

func monthlyKindFromName(name string) monthlyKind {
	lower := strings.ToLower(name)
	if !strings.Contains(lower, "inf_mensal_fii") || !strings.HasSuffix(lower, ".csv") {
		return monthlyKindUnknown
	}
	switch {
	case strings.Contains(lower, "geral"):
		return monthlyKindGeral
	case strings.Contains(lower, "complemento"):
		return monthlyKindComplemento
	case strings.Contains(lower, "ativo_passivo"):
		return monthlyKindAtivoPassivo
	default:
		return monthlyKindUnknown
	}
}

type monthlyKey struct {
	cnpj string
	ref  string
}

type monthlyAccumulator struct {
	record  model.CVMInformeMensal
	records map[string]auditedCSVRow
}

type monthlyPayload struct {
	Records map[string]auditedCSVRow `json:"records"`
}

type auditedCSVRow struct {
	SourceFile string            `json:"source_file"`
	RowNumber  int               `json:"row_number"`
	Headers    map[string]string `json:"headers"`
	Values     map[string]string `json:"values"`
}

func parseMonthlyCSV(
	ctx context.Context,
	r io.Reader,
	sourceFile string,
	kindHint monthlyKind,
	ingested time.Time,
	acc map[monthlyKey]*monthlyAccumulator,
	yieldErr func(error) bool,
) bool {
	reader := csv.NewReader(newUTF8Reader(r))
	reader.Comma = ';'
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return true
		}
		return yieldErr(fmt.Errorf("%s: read header: %w", sourceFile, err))
	}
	cols := indexMonthlyColumns(header)
	kind := kindHint
	if kind == monthlyKindUnknown {
		kind = inferMonthlyKind(cols)
	}
	if cols.cnpj < 0 || cols.reference < 0 {
		return yieldErr(fmt.Errorf("%s: missing CNPJ_Fundo_Classe / Data_Referencia", sourceFile))
	}

	rowNumber := 1
	for {
		if err := ctx.Err(); err != nil {
			return yieldErr(err)
		}
		rowNumber++
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return true
		}
		if err != nil {
			return yieldErr(fmt.Errorf("%s:%d read row: %w", sourceFile, rowNumber, err))
		}
		if err := applyMonthlyRow(kind, cols, header, row, sourceFile, rowNumber, ingested, acc); err != nil {
			if !yieldErr(err) {
				return false
			}
			continue
		}
	}
}

type monthlyColumns struct {
	cnpj                int
	reference           int
	ticker              int
	name                int
	isin                int
	mandate             int
	segment             int
	listedBolsa         int
	administrator       int
	numInvestors        int
	assetsTotal         int
	equityTotal         int
	sharesOutstanding   int
	navPerShare         int
	dividendYieldMonth  int
	amortizationMonth   int
	vacancyPhysical     int
	vacancyFinancial    int
	realEstateTotal     int
	financialAssetTotal int
}

func indexMonthlyColumns(header []string) monthlyColumns {
	idx := monthlyColumns{
		cnpj: -1, reference: -1, ticker: -1, name: -1, isin: -1, mandate: -1,
		segment: -1, listedBolsa: -1, administrator: -1, numInvestors: -1,
		assetsTotal: -1, equityTotal: -1, sharesOutstanding: -1,
		navPerShare: -1, dividendYieldMonth: -1, amortizationMonth: -1,
		vacancyPhysical: -1, vacancyFinancial: -1, realEstateTotal: -1,
		financialAssetTotal: -1,
	}
	for i, raw := range header {
		switch normalizeHeader(raw) {
		case "cnpj_fundo", "cnpj_fundo_classe", "cnpj":
			idx.cnpj = i
		case "data_referencia", "data_competencia":
			idx.reference = i
		case "codigo_b3", "codigo_negociacao", "ticker":
			idx.ticker = i
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
		case "nome_administrador", "administrador":
			idx.administrator = i
		case "total_numero_cotistas", "numero_cotistas", "num_cotistas":
			idx.numInvestors = i
		case "valor_ativo":
			idx.assetsTotal = i
		case "patrimonio_liquido", "patrimonio_total":
			idx.equityTotal = i
		case "cotas_emitidas", "quantidade_cotas_emitidas":
			idx.sharesOutstanding = i
		case "valor_patrimonial_cotas", "valor_patrimonial_cota":
			idx.navPerShare = i
		case "percentual_dividend_yield_mes":
			idx.dividendYieldMonth = i
		case "percentual_amortizacao_cotas_mes":
			idx.amortizationMonth = i
		case "percentual_vacancia", "percentual_imoveis_ocupados_fisicamente", "percentual_ocupacao_fisica":
			idx.vacancyPhysical = i
		case "percentual_imoveis_ocupados_financeiramente", "percentual_ocupacao_financeira":
			idx.vacancyFinancial = i
		case "direitos_bens_imoveis":
			idx.realEstateTotal = i
		case "total_investido":
			idx.financialAssetTotal = i
		}
	}
	return idx
}

func inferMonthlyKind(cols monthlyColumns) monthlyKind {
	switch {
	case cols.listedBolsa >= 0 || cols.isin >= 0 || cols.administrator >= 0:
		return monthlyKindGeral
	case cols.numInvestors >= 0 || cols.navPerShare >= 0 || cols.dividendYieldMonth >= 0:
		return monthlyKindComplemento
	case cols.realEstateTotal >= 0 || cols.financialAssetTotal >= 0:
		return monthlyKindAtivoPassivo
	default:
		return monthlyKindUnknown
	}
}

func applyMonthlyRow(
	kind monthlyKind,
	cols monthlyColumns,
	header []string,
	row []string,
	sourceFile string,
	rowNumber int,
	ingested time.Time,
	acc map[monthlyKey]*monthlyAccumulator,
) error {
	get := func(i int) string {
		if i < 0 || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}
	cnpj := normalizeCNPJ(get(cols.cnpj))
	if cnpj == "" {
		return fmt.Errorf("%s:%d empty cnpj", sourceFile, rowNumber)
	}
	ref, err := parseCVMDate(get(cols.reference))
	if err != nil {
		return fmt.Errorf("%s:%d %s reference date: %w", sourceFile, rowNumber, cnpj, err)
	}
	key := monthlyKey{cnpj: cnpj, ref: ref.Format("2006-01-02")}
	item := acc[key]
	if item == nil {
		item = &monthlyAccumulator{
			record: model.CVMInformeMensal{
				CNPJ:           cnpj,
				ReferenceMonth: ref,
				IngestedAt:     ingested,
			},
			records: map[string]auditedCSVRow{},
		}
		acc[key] = item
	}

	rec := &item.record
	switch kind {
	case monthlyKindGeral:
		mergeString(&rec.Name, get(cols.name))
		mergeString(&rec.ISIN, get(cols.isin))
		if rec.Ticker == "" {
			if raw := get(cols.ticker); raw != "" {
				if t, err := model.ParseTicker(raw); err == nil {
					rec.Ticker = t
				}
			}
			if rec.Ticker == "" {
				rec.Ticker = tickerFromISIN(rec.ISIN)
			}
		}
		mergeString(&rec.Segment, normalizeCVMSegment(get(cols.segment)))
		mergeString(&rec.Mandate, normalizeCVMMandate(get(cols.mandate)))
		mergeString(&rec.Administrator, get(cols.administrator))
		if cols.listedBolsa >= 0 {
			rec.Listed = boolPtr(strings.EqualFold(get(cols.listedBolsa), "S"))
		}
	case monthlyKindComplemento:
		mergeInt(&rec.NumInvestors, parseIntPtr(get(cols.numInvestors)))
		mergeFloat(&rec.AssetsTotal, parseFloatPtr(get(cols.assetsTotal)))
		mergeFloat(&rec.EquityTotal, parseFloatPtr(get(cols.equityTotal)))
		mergeFloat(&rec.SharesOutstanding, parseFloatPtr(get(cols.sharesOutstanding)))
		mergeFloat(&rec.NAVPerShare, parseFloatPtr(get(cols.navPerShare)))
		mergeFloat(&rec.DividendYieldMonth, parsePercentPtr(get(cols.dividendYieldMonth)))
		mergeFloat(&rec.AmortizationMonth, parsePercentPtr(get(cols.amortizationMonth)))
		mergeFloat(&rec.VacancyPhysical, parsePercentPtr(get(cols.vacancyPhysical)))
		mergeFloat(&rec.VacancyFinancial, parsePercentPtr(get(cols.vacancyFinancial)))
	case monthlyKindAtivoPassivo:
		mergeFloat(&rec.RealEstateTotal, parseFloatPtr(get(cols.realEstateTotal)))
		mergeFloat(&rec.FinancialAssetTotal, parseFloatPtr(get(cols.financialAssetTotal)))
		if rec.AssetsTotal == nil {
			mergeFloat(&rec.AssetsTotal, parseFloatPtr(get(cols.financialAssetTotal)))
		}
	default:
		mergeInt(&rec.NumInvestors, parseIntPtr(get(cols.numInvestors)))
		mergeFloat(&rec.EquityTotal, parseFloatPtr(get(cols.equityTotal)))
		mergeFloat(&rec.NAVPerShare, parseFloatPtr(get(cols.navPerShare)))
	}

	item.records[string(kind)] = auditedRow(sourceFile, rowNumber, header, row)
	return nil
}

func auditedRow(sourceFile string, rowNumber int, header, row []string) auditedCSVRow {
	headers := make(map[string]string, len(header))
	values := make(map[string]string, len(header))
	for i, raw := range header {
		key := normalizeHeader(raw)
		headers[key] = raw
		if i < len(row) {
			values[key] = row[i]
		}
	}
	return auditedCSVRow{
		SourceFile: sourceFile,
		RowNumber:  rowNumber,
		Headers:    headers,
		Values:     values,
	}
}

func sortedMonthlyKeys(m map[monthlyKey]*monthlyAccumulator) []monthlyKey {
	keys := make([]monthlyKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].ref == keys[j].ref {
			return keys[i].cnpj < keys[j].cnpj
		}
		return keys[i].ref < keys[j].ref
	})
	return keys
}

func mergeString(dst *string, value string) {
	if *dst == "" && value != "" {
		*dst = value
	}
}

func mergeFloat(dst **float64, value *float64) {
	if *dst == nil && value != nil {
		*dst = value
	}
}

func mergeInt(dst **int64, value *int64) {
	if *dst == nil && value != nil {
		*dst = value
	}
}

func boolPtr(v bool) *bool { return &v }

func parseIntPtr(raw string) *int64 {
	s := normalizeNumber(raw)
	if s == "" {
		return nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		n := int64(f)
		return &n
	}
	return nil
}

func parseFloatPtr(raw string) *float64 {
	s := normalizeNumber(raw)
	if s == "" {
		return nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return &f
	}
	return nil
}

func parsePercentPtr(raw string) *float64 {
	v := parseFloatPtr(raw)
	if v == nil {
		return nil
	}
	if *v > 1 || *v < -1 {
		scaled := *v / 100
		return &scaled
	}
	return v
}

func normalizeNumber(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, ",", ".")
	return s
}

func normalizeHeader(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	s = stripAccents(s)
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, " ", "_")
	return strings.TrimPrefix(s, "\ufeff")
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

func normalizeCVMSegment(raw string) string {
	s := strings.ToLower(stripAccents(strings.TrimSpace(raw)))
	switch {
	case s == "":
		return ""
	case strings.Contains(s, "log"):
		return "logistic"
	case strings.Contains(s, "shopping") || strings.Contains(s, "varejo"):
		return "retail"
	case strings.Contains(s, "laje") || strings.Contains(s, "escritorio") || strings.Contains(s, "corpor"):
		return "office"
	case strings.Contains(s, "titulo") || strings.Contains(s, "valor") || strings.Contains(s, "papel") || strings.Contains(s, "cri"):
		return "paper"
	case strings.Contains(s, "hibr") || strings.Contains(s, "multi"):
		return "hybrid"
	case strings.Contains(s, "fundo de fundo") || strings.Contains(s, "fof"):
		return "fof"
	case strings.Contains(s, "hospital") || strings.Contains(s, "saude"):
		return "healthcare"
	case strings.Contains(s, "residen"):
		return "residential"
	default:
		return "other"
	}
}

func normalizeCVMMandate(raw string) string {
	s := strings.ToLower(stripAccents(strings.TrimSpace(raw)))
	switch {
	case s == "":
		return ""
	case strings.Contains(s, "titulo") || strings.Contains(s, "valor") || strings.Contains(s, "papel"):
		return "paper"
	case strings.Contains(s, "hibr"):
		return "hybrid"
	case strings.Contains(s, "fundo"):
		return "fof"
	default:
		return "brick"
	}
}

// stripAccents does a minimal ASCII fold for the Portuguese headers and
// controlled-value labels used by the CVM CSVs.
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
