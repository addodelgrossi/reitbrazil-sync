-- 0003_views.sql — read-only helper views for common queries.

CREATE VIEW IF NOT EXISTS v_latest_prices AS
SELECT p.*
FROM prices p
JOIN (
    SELECT ticker, MAX(trade_date) AS max_date
    FROM prices
    GROUP BY ticker
) last ON p.ticker = last.ticker AND p.trade_date = last.max_date;

CREATE VIEW IF NOT EXISTS v_upcoming_dividends AS
SELECT *
FROM dividends
WHERE ex_date >= date('now')
ORDER BY ex_date ASC, ticker ASC;
