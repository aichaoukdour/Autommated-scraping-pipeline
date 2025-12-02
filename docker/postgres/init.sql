-- Initialize database schema for ADiL Scraper

-- Main scraped data storage
CREATE TABLE IF NOT EXISTS scraped_data (
    id SERIAL PRIMARY KEY,
    tariff_code VARCHAR(20) NOT NULL,
    data JSONB NOT NULL,
    scraped_at TIMESTAMP NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    data_hash VARCHAR(64),  -- SHA256 hash for deduplication
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_scraped_data_tariff_code ON scraped_data(tariff_code);
CREATE INDEX IF NOT EXISTS idx_scraped_data_scraped_at ON scraped_data(scraped_at);
CREATE INDEX IF NOT EXISTS idx_scraped_data_tariff_version ON scraped_data(tariff_code, version DESC);
CREATE INDEX IF NOT EXISTS idx_scraped_data_jsonb ON scraped_data USING GIN(data);
CREATE INDEX IF NOT EXISTS idx_scraped_data_hash ON scraped_data(data_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_scraped_data_tariff_hash ON scraped_data(tariff_code, data_hash);

-- Change history tracking
CREATE TABLE IF NOT EXISTS data_changes (
    id SERIAL PRIMARY KEY,
    tariff_code VARCHAR(20) NOT NULL,
    change_type VARCHAR(20) NOT NULL,  -- 'created', 'updated'
    old_data JSONB,
    new_data JSONB,
    changes_summary JSONB,
    detected_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_data_changes_tariff_code ON data_changes(tariff_code);
CREATE INDEX IF NOT EXISTS idx_data_changes_detected_at ON data_changes(detected_at);

-- Monitoring activity log
CREATE TABLE IF NOT EXISTS monitoring_log (
    id SERIAL PRIMARY KEY,
    tariff_code VARCHAR(20),
    action VARCHAR(50),  -- 'scraped', 'cached_hit', 'change_detected', 'error'
    status VARCHAR(20),  -- 'success', 'failed', 'skipped'
    duration_ms INTEGER,
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_monitoring_log_tariff_code ON monitoring_log(tariff_code);
CREATE INDEX IF NOT EXISTS idx_monitoring_log_created_at ON monitoring_log(created_at);
CREATE INDEX IF NOT EXISTS idx_monitoring_log_action ON monitoring_log(action);

-- Monitored codes configuration
CREATE TABLE IF NOT EXISTS monitored_codes (
    id SERIAL PRIMARY KEY,
    tariff_code VARCHAR(20) UNIQUE NOT NULL,
    enabled BOOLEAN DEFAULT TRUE,
    interval_minutes INTEGER DEFAULT 60,
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_monitored_codes_enabled ON monitored_codes(enabled);
CREATE INDEX IF NOT EXISTS idx_monitored_codes_priority ON monitored_codes(priority);

-- Comments for documentation
COMMENT ON TABLE scraped_data IS 'Main storage for scraped tariff code data';
COMMENT ON TABLE data_changes IS 'Track changes between scraped data versions';
COMMENT ON TABLE monitoring_log IS 'Log of all monitoring activities';
COMMENT ON TABLE monitored_codes IS 'Configuration for which codes to monitor';

