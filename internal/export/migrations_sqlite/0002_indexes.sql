-- 0002_indexes.sql

CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices(ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_dividends_ex ON dividends(ex_date DESC);
CREATE INDEX IF NOT EXISTS idx_dividends_ticker_ex ON dividends(ticker, ex_date DESC);
CREATE INDEX IF NOT EXISTS idx_calendar_date ON calendar_events(event_date);
CREATE INDEX IF NOT EXISTS idx_snapshots_segment ON fund_snapshots(segment);
CREATE INDEX IF NOT EXISTS idx_fundamentals_ticker_asof ON fundamentals(ticker, as_of DESC);
