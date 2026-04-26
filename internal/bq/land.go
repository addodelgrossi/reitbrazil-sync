package bq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/addodelgrossi/reitbrazil-sync/internal/model"
)

// LandStats summarises one land operation.
type LandStats struct {
	Table        string
	RowsInserted int
	Errors       []error
}

// joinErrors merges LandStats.Errors into a single error via errors.Join.
func (s LandStats) Err() error { return errors.Join(s.Errors...) }

// LandFunds consumes funds and inserts them into raw.brapi_fund_list.
func (c *Client) LandFunds(ctx context.Context, src iter.Seq2[model.Fund, error]) (LandStats, error) {
	ins := c.TableRef(DatasetRaw, TableBrapiFundList).Inserter()
	stats := LandStats{Table: TableBrapiFundList}
	batch := make([]*bigquery.StructSaver, 0, 200)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		savers := make([]any, len(batch))
		for i, b := range batch {
			savers[i] = b
		}
		if err := ins.Put(ctx, savers); err != nil {
			return err
		}
		stats.RowsInserted += len(batch)
		batch = batch[:0]
		return nil
	}

	for f, err := range src {
		if err != nil {
			stats.Errors = append(stats.Errors, err)
			continue
		}
		payload := f.Payload
		if len(payload) == 0 {
			payload, _ = json.Marshal(struct {
				Ticker string `json:"ticker"`
				Name   string `json:"name"`
			}{string(f.Ticker), f.Name})
		}
		row := &bigquery.StructSaver{
			Schema:   RawSchemas()[TableBrapiFundList],
			InsertID: fmt.Sprintf("%s-%d", f.Ticker, ingestedOrNow(f.IngestedAt).UnixNano()),
			Struct: rawFundRow{
				Ticker:        string(f.Ticker),
				CNPJ:          f.CNPJ,
				ISIN:          f.ISIN,
				LongName:      f.Name,
				Segment:       f.Segment,
				Mandate:       f.Mandate,
				Manager:       f.Manager,
				Administrator: f.Administrator,
				Listed:        &f.Listed,
				Payload:       string(payload),
				IngestedAt:    ingestedOrNow(f.IngestedAt),
			},
		}
		batch = append(batch, row)
		if len(batch) >= 200 {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

// LandPrices inserts OHLCV bars into raw.brapi_quote.
func (c *Client) LandPrices(ctx context.Context, src iter.Seq2[model.PriceBar, error]) (LandStats, error) {
	ins := c.TableRef(DatasetRaw, TableBrapiQuote).Inserter()
	stats := LandStats{Table: TableBrapiQuote}
	batch := make([]*bigquery.StructSaver, 0, 500)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		savers := make([]any, len(batch))
		for i, b := range batch {
			savers[i] = b
		}
		if err := ins.Put(ctx, savers); err != nil {
			return err
		}
		stats.RowsInserted += len(batch)
		batch = batch[:0]
		return nil
	}

	for pb, err := range src {
		if err != nil {
			stats.Errors = append(stats.Errors, err)
			continue
		}
		payload := pb.Payload
		if len(payload) == 0 {
			payload = []byte(`{}`)
		}
		row := &bigquery.StructSaver{
			Schema:   RawSchemas()[TableBrapiQuote],
			InsertID: fmt.Sprintf("%s-%s-%d", pb.Ticker, pb.TradeDate.Format("2006-01-02"), ingestedOrNow(pb.IngestedAt).UnixNano()),
			Struct: rawQuoteRow{
				Ticker:     string(pb.Ticker),
				TradeDate:  civilDate(pb.TradeDate),
				Open:       pb.Open,
				High:       pb.High,
				Low:        pb.Low,
				Close:      pb.Close,
				Volume:     pb.Volume,
				Payload:    string(payload),
				IngestedAt: ingestedOrNow(pb.IngestedAt),
			},
		}
		batch = append(batch, row)
		if len(batch) >= 500 {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

// LandDividends inserts dividend events into raw.brapi_dividends.
func (c *Client) LandDividends(ctx context.Context, src iter.Seq2[model.Dividend, error]) (LandStats, error) {
	ins := c.TableRef(DatasetRaw, TableBrapiDividends).Inserter()
	stats := LandStats{Table: TableBrapiDividends}
	batch := make([]*bigquery.StructSaver, 0, 500)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		savers := make([]any, len(batch))
		for i, b := range batch {
			savers[i] = b
		}
		if err := ins.Put(ctx, savers); err != nil {
			return err
		}
		stats.RowsInserted += len(batch)
		batch = batch[:0]
		return nil
	}

	for d, err := range src {
		if err != nil {
			stats.Errors = append(stats.Errors, err)
			continue
		}
		payload := d.Payload
		if len(payload) == 0 {
			payload = []byte(`{}`)
		}
		eventID := distributionEventID(d)
		row := &bigquery.StructSaver{
			Schema:   RawSchemas()[TableBrapiDividends],
			InsertID: fmt.Sprintf("%s-%d", eventID, ingestedOrNow(d.IngestedAt).UnixNano()),
			Struct: rawDividendRow{
				EventID:      eventID,
				Ticker:       string(d.Ticker),
				ExDate:       civilDate(d.ExDate),
				AnnounceDate: nullableCivilDate(d.AnnounceDate),
				RecordDate:   nullableCivilDate(d.RecordDate),
				PaymentDate:  nullableCivilDate(d.PaymentDate),
				Amount:       d.AmountPerShare,
				Kind:         string(d.Kind),
				Source:       d.Source,
				Payload:      string(payload),
				IngestedAt:   ingestedOrNow(d.IngestedAt),
			},
		}
		batch = append(batch, row)
		if len(batch) >= 500 {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

// LandFundamentals inserts one fundamentals snapshot.
func (c *Client) LandFundamentals(ctx context.Context, f model.Fundamentals) (LandStats, error) {
	ins := c.TableRef(DatasetRaw, TableBrapiFundamentals).Inserter()
	stats := LandStats{Table: TableBrapiFundamentals}
	payload := f.Payload
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	row := &bigquery.StructSaver{
		Schema:   RawSchemas()[TableBrapiFundamentals],
		InsertID: fmt.Sprintf("%s-%s-%d", f.Ticker, f.AsOf.Format("2006-01-02"), ingestedOrNow(f.IngestedAt).UnixNano()),
		Struct: rawFundamentalsRow{
			Ticker:           string(f.Ticker),
			AsOf:             civilDate(f.AsOf),
			NAVPerShare:      f.NAVPerShare,
			PVP:              f.PVP,
			AssetsTotal:      f.AssetsTotal,
			EquityTotal:      f.EquityTotal,
			NumInvestors:     f.NumInvestors,
			Liquidity90d:     f.Liquidity90d,
			VacancyPhysical:  f.VacancyPhysical,
			VacancyFinancial: f.VacancyFinancial,
			OccupancyRate:    f.OccupancyRate,
			Payload:          string(payload),
			IngestedAt:       ingestedOrNow(f.IngestedAt),
		},
	}
	if err := ins.Put(ctx, []any{row}); err != nil {
		return stats, err
	}
	stats.RowsInserted = 1
	return stats, nil
}

// LandCVMInforme inserts a stream of CVM informes.
func (c *Client) LandCVMInforme(ctx context.Context, src iter.Seq2[model.CVMInformeMensal, error]) (LandStats, error) {
	ins := c.TableRef(DatasetRaw, TableCVMInformeMensal).Inserter()
	stats := LandStats{Table: TableCVMInformeMensal}
	batch := make([]*bigquery.StructSaver, 0, 500)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		savers := make([]any, len(batch))
		for i, b := range batch {
			savers[i] = b
		}
		if err := ins.Put(ctx, savers); err != nil {
			return err
		}
		stats.RowsInserted += len(batch)
		batch = batch[:0]
		return nil
	}

	for rec, err := range src {
		if err != nil {
			stats.Errors = append(stats.Errors, err)
			continue
		}
		payload := rec.Payload
		if len(payload) == 0 {
			payload = []byte(`{}`)
		}
		row := &bigquery.StructSaver{
			Schema:   RawSchemas()[TableCVMInformeMensal],
			InsertID: fmt.Sprintf("%s-%s-%d", rec.CNPJ, rec.ReferenceMonth.Format("2006-01-02"), ingestedOrNow(rec.IngestedAt).UnixNano()),
			Struct: rawCVMInformeRow{
				CNPJ:                rec.CNPJ,
				Ticker:              string(rec.Ticker),
				ReferenceMonth:      civilDate(rec.ReferenceMonth),
				Name:                rec.Name,
				ISIN:                rec.ISIN,
				Segment:             rec.Segment,
				Mandate:             rec.Mandate,
				Administrator:       rec.Administrator,
				Listed:              rec.Listed,
				NumInvestors:        rec.NumInvestors,
				AssetsTotal:         rec.AssetsTotal,
				EquityTotal:         rec.EquityTotal,
				SharesOutstanding:   rec.SharesOutstanding,
				NAVPerShare:         rec.NAVPerShare,
				DividendYieldMonth:  rec.DividendYieldMonth,
				AmortizationMonth:   rec.AmortizationMonth,
				VacancyPhysical:     rec.VacancyPhysical,
				VacancyFinancial:    rec.VacancyFinancial,
				RealEstateTotal:     rec.RealEstateTotal,
				FinancialAssetTotal: rec.FinancialAssetTotal,
				Payload:             string(payload),
				IngestedAt:          ingestedOrNow(rec.IngestedAt),
			},
		}
		batch = append(batch, row)
		if len(batch) >= 500 {
			if err := flush(); err != nil {
				return stats, err
			}
		}
	}
	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

func ingestedOrNow(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t
}

func distributionEventID(d model.Distribution) string {
	payment := ""
	if d.PaymentDate != nil {
		payment = d.PaymentDate.UTC().Format("2006-01-02")
	}
	source := d.Source
	if source == "" {
		source = "unknown"
	}
	return fmt.Sprintf("%s:%s:%s:%s:%.12g",
		d.Ticker,
		d.ExDate.UTC().Format("2006-01-02"),
		payment,
		d.Kind,
		d.AmountPerShare,
	) + ":" + source
}
