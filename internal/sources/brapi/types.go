package brapi

// Internal DTOs mirroring the brapi.dev JSON envelope. These do not
// escape the package; callers consume model.* types.

type quoteListResponse struct {
	Stocks      []fundListItem `json:"stocks"`
	HasNextPage bool           `json:"hasNextPage"`
}

type fundListItem struct {
	Stock     string  `json:"stock"`
	Name      string  `json:"name"`
	LongName  string  `json:"longName,omitempty"`
	Type      string  `json:"type,omitempty"`
	Close     float64 `json:"close,omitempty"`
	Volume    int64   `json:"volume,omitempty"`
	Sector    string  `json:"sector,omitempty"`
}

type quoteDetailResponse struct {
	Results []quoteDetail `json:"results"`
}

type quoteDetail struct {
	Symbol                      string                `json:"symbol"`
	LongName                    string                `json:"longName,omitempty"`
	ShortName                   string                `json:"shortName,omitempty"`
	Currency                    string                `json:"currency,omitempty"`
	RegularMarketPrice          float64               `json:"regularMarketPrice,omitempty"`
	RegularMarketDayHigh        float64               `json:"regularMarketDayHigh,omitempty"`
	RegularMarketDayLow         float64               `json:"regularMarketDayLow,omitempty"`
	RegularMarketOpen           float64               `json:"regularMarketOpen,omitempty"`
	RegularMarketVolume         int64                 `json:"regularMarketVolume,omitempty"`
	HistoricalDataPrice         []quoteHistoricalBar  `json:"historicalDataPrice,omitempty"`
	DividendsData               *dividendsEnvelope    `json:"dividendsData,omitempty"`
	DefaultKeyStatistics        *keyStatistics        `json:"defaultKeyStatistics,omitempty"`
	FinancialData               *financialData        `json:"financialData,omitempty"`
}

type quoteHistoricalBar struct {
	Date           int64   `json:"date"` // unix seconds
	Open           float64 `json:"open"`
	High           float64 `json:"high"`
	Low            float64 `json:"low"`
	Close          float64 `json:"close"`
	Volume         int64   `json:"volume"`
	AdjustedClose  float64 `json:"adjustedClose,omitempty"`
}

type dividendsEnvelope struct {
	CashDividends []cashDividend `json:"cashDividends"`
}

type cashDividend struct {
	AssetIssued  string  `json:"assetIssued,omitempty"`
	PaymentDate  string  `json:"paymentDate,omitempty"`  // YYYY-MM-DD
	Rate         float64 `json:"rate"`
	RelatedTo    string  `json:"relatedTo,omitempty"`
	ApprovedOn   string  `json:"approvedOn,omitempty"`
	IsinCode     string  `json:"isinCode,omitempty"`
	Label        string  `json:"label,omitempty"` // DIVIDEND, AMORTIZATION, RIGHTS, etc.
	LastDatePrior string `json:"lastDatePrior,omitempty"` // ex-date
	RecordDate   string  `json:"recordDate,omitempty"`
}

type keyStatistics struct {
	BookValue          float64 `json:"bookValue,omitempty"`
	PriceToBook        float64 `json:"priceToBook,omitempty"`
	SharesOutstanding  float64 `json:"sharesOutstanding,omitempty"`
	LastDividendValue  float64 `json:"lastDividendValue,omitempty"`
	LastDividendDate   int64   `json:"lastDividendDate,omitempty"`
}

type financialData struct {
	TotalAssets      float64 `json:"totalAssets,omitempty"`
	TotalRevenue     float64 `json:"totalRevenue,omitempty"`
	RevenuePerShare  float64 `json:"revenuePerShare,omitempty"`
	CurrentPrice     float64 `json:"currentPrice,omitempty"`
	ReturnOnEquity   float64 `json:"returnOnEquity,omitempty"`
}
