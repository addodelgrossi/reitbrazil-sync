-- 0001_init.sql
-- Base schema for the reitbrazil read-only MCP server.

CREATE TABLE IF NOT EXISTS funds (
    ticker              TEXT PRIMARY KEY,
    cnpj                TEXT UNIQUE,
    name                TEXT NOT NULL,
    segment             TEXT,
    mandate             TEXT,
    manager             TEXT,
    administrator       TEXT,
    ipo_date            TEXT,
    listed              INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS prices (
    ticker              TEXT NOT NULL,
    trade_date          TEXT NOT NULL,
    open                REAL,
    high                REAL,
    low                 REAL,
    close               REAL NOT NULL,
    volume              INTEGER,
    PRIMARY KEY (ticker, trade_date),
    FOREIGN KEY (ticker) REFERENCES funds(ticker)
);

CREATE TABLE IF NOT EXISTS dividends (
    ticker              TEXT NOT NULL,
    announce_date       TEXT,
    ex_date             TEXT NOT NULL,
    record_date         TEXT,
    payment_date        TEXT,
    amount_per_share    REAL NOT NULL,
    kind                TEXT NOT NULL DEFAULT 'dividend',
    source              TEXT,
    PRIMARY KEY (ticker, ex_date, kind),
    FOREIGN KEY (ticker) REFERENCES funds(ticker)
);

CREATE TABLE IF NOT EXISTS fundamentals (
    ticker              TEXT NOT NULL,
    as_of               TEXT NOT NULL,
    nav_per_share       REAL,
    pvp                 REAL,
    assets_total        REAL,
    equity_total        REAL,
    num_investors       INTEGER,
    liquidity_90d       REAL,
    vacancy_physical    REAL,
    vacancy_financial   REAL,
    occupancy_rate      REAL,
    PRIMARY KEY (ticker, as_of),
    FOREIGN KEY (ticker) REFERENCES funds(ticker)
);

CREATE TABLE IF NOT EXISTS calendar_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT,
    event_date          TEXT NOT NULL,
    kind                TEXT NOT NULL,
    description         TEXT,
    source              TEXT
);

CREATE TABLE IF NOT EXISTS data_sources (
    name                TEXT PRIMARY KEY,
    last_refreshed_at   TEXT NOT NULL,
    coverage_from       TEXT,
    coverage_to         TEXT,
    notes               TEXT
);

CREATE TABLE IF NOT EXISTS fund_snapshots (
    ticker               TEXT PRIMARY KEY,
    last_close           REAL,
    last_close_date      TEXT,
    dy_trailing_12m      REAL,
    dy_forward_est       REAL,
    avg_daily_volume_90d REAL,
    volatility_90d       REAL,
    max_drawdown_1y      REAL,
    pvp                  REAL,
    segment              TEXT,
    mandate              TEXT,
    updated_at           TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (ticker) REFERENCES funds(ticker)
);
