-- -- -- Create stock_data database if it doesn't exist
-- -- -- This is handled by the create-multiple-dbs.sh script

-- -- -- Connect to the stock_data database
-- -- \c stock_data;

-- -- -- Create schema for stock market data
-- -- CREATE SCHEMA IF NOT EXISTS stock_market;

-- -- -- Create table for storing stock market data
-- -- CREATE TABLE IF NOT EXISTS stock_market.daily_prices (
-- --     id SERIAL PRIMARY KEY,
-- --     symbol VARCHAR(10) NOT NULL,
-- --     date DATE NOT NULL,
-- --     open_price DECIMAL(10, 4) NOT NULL,
-- --     high_price DECIMAL(10, 4) NOT NULL,
-- --     low_price DECIMAL(10, 4) NOT NULL,
-- --     close_price DECIMAL(10, 4) NOT NULL,
-- --     volume BIGINT NOT NULL,
-- --     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
-- --     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
-- --     -- Ensure unique constraint on symbol and date
-- --     CONSTRAINT unique_symbol_date UNIQUE (symbol, date)
-- -- );

-- -- -- Create indexes for better query performance
-- -- CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol ON stock_market.daily_prices(symbol);
-- -- CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON stock_market.daily_prices(date);
-- -- CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol_date ON stock_market.daily_prices(symbol, date);

-- -- -- Create table for storing intraday data (optional for future use)
-- -- CREATE TABLE IF NOT EXISTS stock_market.intraday_prices (
-- --     id SERIAL PRIMARY KEY,
-- --     symbol VARCHAR(10) NOT NULL,
-- --     timestamp TIMESTAMP NOT NULL,
-- --     open_price DECIMAL(10, 4) NOT NULL,
-- --     high_price DECIMAL(10, 4) NOT NULL,
-- --     low_price DECIMAL(10, 4) NOT NULL,
-- --     close_price DECIMAL(10, 4) NOT NULL,
-- --     volume BIGINT NOT NULL,
-- --     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
-- --     -- Ensure unique constraint on symbol and timestamp
-- --     CONSTRAINT unique_symbol_timestamp UNIQUE (symbol, timestamp)
-- -- );

-- -- -- Create indexes for intraday data
-- -- CREATE INDEX IF NOT EXISTS idx_intraday_prices_symbol ON stock_market.intraday_prices(symbol);
-- -- CREATE INDEX IF NOT EXISTS idx_intraday_prices_timestamp ON stock_market.intraday_prices(timestamp);
-- -- CREATE INDEX IF NOT EXISTS idx_intraday_prices_symbol_timestamp ON stock_market.intraday_prices(symbol, timestamp);

-- -- -- Create table for tracking API calls and pipeline runs
-- -- CREATE TABLE IF NOT EXISTS stock_market.pipeline_runs (
-- --     id SERIAL PRIMARY KEY,
-- --     run_id VARCHAR(100) UNIQUE NOT NULL,
-- --     dag_id VARCHAR(100) NOT NULL,
-- --     task_id VARCHAR(100) NOT NULL,
-- --     symbol VARCHAR(10),
-- --     start_time TIMESTAMP NOT NULL,
-- --     end_time TIMESTAMP,
-- --     status VARCHAR(20) NOT NULL DEFAULT 'running',
-- --     records_processed INTEGER DEFAULT 0,
-- --     error_message TEXT,
-- --     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- -- );

-- -- -- Create index for pipeline runs
-- -- CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON stock_market.pipeline_runs(status);
-- -- CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_task ON stock_market.pipeline_runs(dag_id, task_id);

-- -- -- Create function to update the updated_at column
-- -- CREATE OR REPLACE FUNCTION update_updated_at_column()
-- -- RETURNS TRIGGER AS $$
-- -- BEGIN
-- --     NEW.updated_at = CURRENT_TIMESTAMP;
-- --     RETURN NEW;
-- -- END;
-- -- $$ language 'plpgsql';

-- -- -- Create trigger to automatically update the updated_at column
-- -- DROP TRIGGER IF EXISTS update_daily_prices_updated_at ON stock_market.daily_prices;
-- -- CREATE TRIGGER update_daily_prices_updated_at 
-- --     BEFORE UPDATE ON stock_market.daily_prices 
-- --     FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- -- -- Insert some example data (optional)
-- -- INSERT INTO stock_market.daily_prices (symbol, date, open_price, high_price, low_price, close_price, volume)
-- -- VALUES 
-- --     ('AAPL', CURRENT_DATE - INTERVAL '1 day', 150.00, 155.00, 149.00, 154.00, 1000000),
-- --     ('GOOGL', CURRENT_DATE - INTERVAL '1 day', 2800.00, 2850.00, 2790.00, 2840.00, 500000)
-- -- ON CONFLICT (symbol, date) DO NOTHING;

-- -- -- Grant permissions (if needed for specific users)
-- -- -- GRANT ALL PRIVILEGES ON SCHEMA stock_market TO your_user;
-- -- -- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA stock_market TO your_user;
-- -- -- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA stock_market TO your_user;






-- -- ================================
-- -- Additional Tables for Forecasts & Raw Data
-- -- ================================

-- -- Raw ingestion table (staging before cleaning/transform)
-- CREATE TABLE IF NOT EXISTS stock_market.raw_prices (
--     id SERIAL PRIMARY KEY,
--     symbol VARCHAR(10) NOT NULL,
--     trade_date DATE NOT NULL,
--     open_price DECIMAL(10, 4),
--     high_price DECIMAL(10, 4),
--     low_price DECIMAL(10, 4),
--     close_price DECIMAL(10, 4),
--     volume BIGINT,
--     ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
--     CONSTRAINT unique_symbol_trade_date UNIQUE (symbol, trade_date)
-- );

-- CREATE INDEX IF NOT EXISTS idx_raw_prices_symbol_date 
--     ON stock_market.raw_prices(symbol, trade_date);

-- -- Forecast / Predictions table
-- CREATE TABLE IF NOT EXISTS stock_market.predictions (
--     id SERIAL PRIMARY KEY,
--     symbol VARCHAR(10) NOT NULL,
--     forecast_date DATE NOT NULL,
--     predicted_close DECIMAL(10, 4) NOT NULL,
--     model_used VARCHAR(50) NOT NULL,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
--     CONSTRAINT unique_symbol_forecast UNIQUE (symbol, forecast_date, model_used)
-- );

-- CREATE INDEX IF NOT EXISTS idx_predictions_symbol_date 
--     ON stock_market.predictions(symbol, forecast_date);

-- -- ================================
-- -- (Optional) User/Access Management
-- -- ================================
-- CREATE TABLE IF NOT EXISTS stock_market.users (
--     id SERIAL PRIMARY KEY,
--     username VARCHAR(50) UNIQUE NOT NULL,
--     password_hash TEXT NOT NULL,
--     role VARCHAR(20) NOT NULL DEFAULT 'viewer',  -- e.g. admin, analyst, viewer
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

-- -- Example user (replace password_hash with hashed value in real setup)
-- INSERT INTO stock_market.users (username, password_hash, role)
-- VALUES ('admin', 'changeme_hash', 'admin')
-- ON CONFLICT (username) DO NOTHING;








-- Connect to stock_data database
\c stock_data;

-- ============================
-- Schema: stock_market
-- ============================
CREATE SCHEMA IF NOT EXISTS stock_market;

-- Daily Prices Table
CREATE TABLE IF NOT EXISTS stock_market.daily_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(10, 4) NOT NULL,
    high_price DECIMAL(10, 4) NOT NULL,
    low_price DECIMAL(10, 4) NOT NULL,
    close_price DECIMAL(10, 4) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_symbol_date UNIQUE (symbol, date)
);

-- Indexes for daily prices
CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol ON stock_market.daily_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON stock_market.daily_prices(date);
CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol_date ON stock_market.daily_prices(symbol, date);

-- Intraday Prices Table
CREATE TABLE IF NOT EXISTS stock_market.intraday_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open_price DECIMAL(10, 4) NOT NULL,
    high_price DECIMAL(10, 4) NOT NULL,
    low_price DECIMAL(10, 4) NOT NULL,
    close_price DECIMAL(10, 4) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_symbol_timestamp UNIQUE (symbol, timestamp)
);

-- Indexes for intraday prices
CREATE INDEX IF NOT EXISTS idx_intraday_prices_symbol ON stock_market.intraday_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_intraday_prices_timestamp ON stock_market.intraday_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_intraday_prices_symbol_timestamp ON stock_market.intraday_prices(symbol, timestamp);

-- Pipeline Runs Table
CREATE TABLE IF NOT EXISTS stock_market.pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) UNIQUE NOT NULL,
    dag_id VARCHAR(100) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(10),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for pipeline runs
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON stock_market.pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_task ON stock_market.pipeline_runs(dag_id, task_id);

-- Update trigger for daily_prices.updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_daily_prices_updated_at ON stock_market.daily_prices;
CREATE TRIGGER update_daily_prices_updated_at 
    BEFORE UPDATE ON stock_market.daily_prices 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ============================
-- Schema: analytics
-- ============================
CREATE SCHEMA IF NOT EXISTS analytics;

-- Companies Table
CREATE TABLE IF NOT EXISTS analytics.companies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    industry VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stock Prices Table
CREATE TABLE IF NOT EXISTS analytics.stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price_date DATE NOT NULL,
    open_price DECIMAL(10, 4),
    high_price DECIMAL(10, 4),
    low_price DECIMAL(10, 4),
    close_price DECIMAL(10, 4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_symbol_date UNIQUE (symbol, price_date)
);

-- Indexes for stock_prices
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date 
    ON analytics.stock_prices(symbol, price_date);

-- ETL Logs Table
CREATE TABLE IF NOT EXISTS analytics.etl_logs (
    id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    records_loaded INTEGER DEFAULT 0,
    error_message TEXT
);


-- ============================
-- Sample Data
-- ============================
INSERT INTO stock_market.daily_prices (symbol, date, open_price, high_price, low_price, close_price, volume)
VALUES 
    ('AAPL', CURRENT_DATE - INTERVAL '1 day', 150.00, 155.00, 149.00, 154.00, 1000000),
    ('GOOGL', CURRENT_DATE - INTERVAL '1 day', 2800.00, 2850.00, 2790.00, 2840.00, 500000)
ON CONFLICT (symbol, date) DO NOTHING;

INSERT INTO analytics.companies (symbol, name, sector, industry)
VALUES
    ('AAPL', 'Apple Inc.', 'Technology', 'Consumer Electronics'),
    ('GOOGL', 'Alphabet Inc.', 'Technology', 'Internet Services')
ON CONFLICT (symbol) DO NOTHING;





