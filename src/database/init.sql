-- ============================================
-- TimescaleDB Initialization fŸr S&P 500 Stock Data
-- ============================================

-- Aktiviere TimescaleDB Extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================
-- TICKERS TABLE (S&P 500 Metadata)
-- ============================================

CREATE TABLE IF NOT EXISTS tickers (
    symbol VARCHAR(10) PRIMARY KEY,
    security VARCHAR(255) NOT NULL,
    gics_sector VARCHAR(100),
    gics_sub_industry VARCHAR(100),
    headquarters_location VARCHAR(255),
    date_added DATE,
    cik INTEGER,
    founded VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tickers_sector ON tickers(gics_sector);
CREATE INDEX IF NOT EXISTS idx_tickers_symbol ON tickers(symbol);

-- ============================================
-- DAILY BARS TABLE (1 Day Candles seit 2020)
-- ============================================

CREATE TABLE IF NOT EXISTS daily_bars (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    vwap DOUBLE PRECISION,
    transactions INTEGER,
    PRIMARY KEY (time, symbol)
);

-- Erstelle Hypertable (wenn noch nicht erstellt)
SELECT create_hypertable('daily_bars', 'time', if_not_exists => TRUE);

-- Indexes fŸr Performance
CREATE INDEX IF NOT EXISTS idx_daily_bars_symbol ON daily_bars(symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_daily_bars_time ON daily_bars(time DESC);

-- ============================================
-- MINUTE BARS TABLE (1 Minute Candles seit 2023)
-- ============================================

CREATE TABLE IF NOT EXISTS minute_bars (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    vwap DOUBLE PRECISION,
    transactions INTEGER,
    PRIMARY KEY (time, symbol)
);

-- Erstelle Hypertable
SELECT create_hypertable('minute_bars', 'time', if_not_exists => TRUE);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_minute_bars_symbol ON minute_bars(symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_minute_bars_time ON minute_bars(time DESC);

-- ============================================
-- DOWNLOAD STATUS TABLE (Progress Tracking)
-- ============================================

CREATE TABLE IF NOT EXISTS download_status (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timespan VARCHAR(20) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    bars_downloaded INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_download UNIQUE (symbol, timespan)
);

CREATE INDEX IF NOT EXISTS idx_download_status_symbol ON download_status(symbol);
CREATE INDEX IF NOT EXISTS idx_download_status_status ON download_status(status);

-- ============================================
-- HELPER FUNCTIONS
-- ============================================

-- Funktion: Get Data Range
CREATE OR REPLACE FUNCTION get_data_range(ticker VARCHAR(10), ts VARCHAR(20))
RETURNS TABLE(min_date TIMESTAMPTZ, max_date TIMESTAMPTZ, count BIGINT) AS $$
BEGIN
    IF ts = 'day' THEN
        RETURN QUERY
        SELECT MIN(time), MAX(time), COUNT(*)
        FROM daily_bars
        WHERE symbol = ticker;
    ELSE
        RETURN QUERY
        SELECT MIN(time), MAX(time), COUNT(*)
        FROM minute_bars
        WHERE symbol = ticker;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Funktion: Check if data exists
CREATE OR REPLACE FUNCTION has_data(ticker VARCHAR(10), ts VARCHAR(20))
RETURNS BOOLEAN AS $$
DECLARE
    cnt BIGINT;
BEGIN
    IF ts = 'day' THEN
        SELECT COUNT(*) INTO cnt FROM daily_bars WHERE symbol = ticker LIMIT 1;
    ELSE
        SELECT COUNT(*) INTO cnt FROM minute_bars WHERE symbol = ticker LIMIT 1;
    END IF;
    RETURN cnt > 0;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- SUCCESS MESSAGE
-- ============================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '  TimescaleDB Schema initialized successfully!';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
    RAISE NOTICE '  Tables created:';
    RAISE NOTICE '    ¥ tickers            (S&P 500 Metadata)';
    RAISE NOTICE '    ¥ daily_bars         (Hypertable - Daily Candles)';
    RAISE NOTICE '    ¥ minute_bars        (Hypertable - Minute Candles)';
    RAISE NOTICE '    ¥ download_status    (Download Progress)';
    RAISE NOTICE '';
    RAISE NOTICE '  Helper Functions:';
    RAISE NOTICE '    ¥ get_data_range(ticker, timespan)';
    RAISE NOTICE '    ¥ has_data(ticker, timespan)';
    RAISE NOTICE '';
    RAISE NOTICE '  Ready for S&P 500 data ingestion!';
    RAISE NOTICE '============================================================';
    RAISE NOTICE '';
END
$$;
