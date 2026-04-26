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
	"strings"
	"time"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

// ParsePropertyVacancy reads inf_trimestral_fii_imovel_*.csv files and
// yields property-level vacancy observations. Monthly informes do not
// publish true Percentual_Vacancia, so this parser is the authoritative
// path for physical vacancy once the quarterly source is wired into land.
func ParsePropertyVacancy(ctx context.Context, zipBytes []byte) iter.Seq2[model.CVMPropertyVacancy, error] {
	return func(yield func(model.CVMPropertyVacancy, error) bool) {
		r, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
		if err != nil {
			yield(model.CVMPropertyVacancy{}, fmt.Errorf("open zip: %w", err))
			return
		}
		ingested := time.Now().UTC()
		for _, f := range r.File {
			name := strings.ToLower(f.Name)
			if !strings.Contains(name, "inf_trimestral_fii_imovel_") || !strings.HasSuffix(name, ".csv") {
				continue
			}
			if strings.Contains(name, "contrato") || strings.Contains(name, "inquilino") {
				continue
			}
			if err := ctx.Err(); err != nil {
				yield(model.CVMPropertyVacancy{}, err)
				return
			}
			rc, err := f.Open()
			if err != nil {
				yield(model.CVMPropertyVacancy{}, fmt.Errorf("open %s: %w", f.Name, err))
				return
			}
			ok := parsePropertyVacancyCSV(ctx, rc, f.Name, ingested, yield)
			if err := rc.Close(); err != nil && ok {
				ok = yield(model.CVMPropertyVacancy{}, fmt.Errorf("close %s: %w", f.Name, err))
			}
			if !ok {
				return
			}
		}
	}
}

// ParsePropertyVacancyCSV parses an already-opened trimestral imovel CSV.
func ParsePropertyVacancyCSV(ctx context.Context, r io.Reader) iter.Seq2[model.CVMPropertyVacancy, error] {
	return func(yield func(model.CVMPropertyVacancy, error) bool) {
		parsePropertyVacancyCSV(ctx, r, "inline.csv", time.Now().UTC(), yield)
	}
}

type propertyVacancyCols struct {
	cnpj          int
	reference     int
	propertyName  int
	propertyClass int
	vacancy       int
	delinquency   int
	revenueShare  int
	leasedShare   int
	soldShare     int
}

func parsePropertyVacancyCSV(
	ctx context.Context,
	r io.Reader,
	sourceFile string,
	ingested time.Time,
	yield func(model.CVMPropertyVacancy, error) bool,
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
		return yield(model.CVMPropertyVacancy{}, fmt.Errorf("%s: read header: %w", sourceFile, err))
	}
	cols := indexPropertyVacancyColumns(header)
	if cols.cnpj < 0 || cols.reference < 0 {
		return yield(model.CVMPropertyVacancy{}, fmt.Errorf("%s: missing CNPJ_Fundo_Classe / Data_Referencia", sourceFile))
	}

	rowNumber := 1
	for {
		if err := ctx.Err(); err != nil {
			return yield(model.CVMPropertyVacancy{}, err)
		}
		rowNumber++
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return true
		}
		if err != nil {
			return yield(model.CVMPropertyVacancy{}, fmt.Errorf("%s:%d read row: %w", sourceFile, rowNumber, err))
		}
		rec, err := mapPropertyVacancyRow(cols, header, row, sourceFile, rowNumber, ingested)
		if err != nil {
			if !yield(model.CVMPropertyVacancy{}, err) {
				return false
			}
			continue
		}
		if !yield(rec, nil) {
			return false
		}
	}
}

func indexPropertyVacancyColumns(header []string) propertyVacancyCols {
	idx := propertyVacancyCols{
		cnpj: -1, reference: -1, propertyName: -1, propertyClass: -1,
		vacancy: -1, delinquency: -1, revenueShare: -1, leasedShare: -1, soldShare: -1,
	}
	for i, raw := range header {
		switch normalizeHeader(raw) {
		case "cnpj_fundo_classe", "cnpj_fundo", "cnpj":
			idx.cnpj = i
		case "data_referencia":
			idx.reference = i
		case "nome_imovel":
			idx.propertyName = i
		case "classe":
			idx.propertyClass = i
		case "percentual_vacancia":
			idx.vacancy = i
		case "percentual_inadimplencia":
			idx.delinquency = i
		case "percentual_receitas_fii":
			idx.revenueShare = i
		case "percentual_locado":
			idx.leasedShare = i
		case "percentual_vendido":
			idx.soldShare = i
		}
	}
	return idx
}

func mapPropertyVacancyRow(
	cols propertyVacancyCols,
	header []string,
	row []string,
	sourceFile string,
	rowNumber int,
	ingested time.Time,
) (model.CVMPropertyVacancy, error) {
	get := func(i int) string {
		if i < 0 || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}
	cnpj := normalizeCNPJ(get(cols.cnpj))
	if cnpj == "" {
		return model.CVMPropertyVacancy{}, fmt.Errorf("%s:%d empty cnpj", sourceFile, rowNumber)
	}
	ref, err := parseCVMDate(get(cols.reference))
	if err != nil {
		return model.CVMPropertyVacancy{}, fmt.Errorf("%s:%d %s reference date: %w", sourceFile, rowNumber, cnpj, err)
	}
	payload, err := json.Marshal(auditedRow(sourceFile, rowNumber, header, row))
	if err != nil {
		return model.CVMPropertyVacancy{}, fmt.Errorf("%s:%d payload: %w", sourceFile, rowNumber, err)
	}
	return model.CVMPropertyVacancy{
		CNPJ:             cnpj,
		ReferenceQuarter: ref,
		PropertyName:     get(cols.propertyName),
		PropertyClass:    get(cols.propertyClass),
		VacancyPhysical:  parsePercentPtr(get(cols.vacancy)),
		DelinquencyRate:  parsePercentPtr(get(cols.delinquency)),
		RevenueShare:     parsePercentPtr(get(cols.revenueShare)),
		LeasedShare:      parsePercentPtr(get(cols.leasedShare)),
		SoldShare:        parsePercentPtr(get(cols.soldShare)),
		Payload:          payload,
		IngestedAt:       ingested,
	}, nil
}
