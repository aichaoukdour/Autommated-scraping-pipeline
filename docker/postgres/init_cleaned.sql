-- Cleaned data table for transformed/normalized data
CREATE TABLE IF NOT EXISTS cleaned_data (
    id SERIAL PRIMARY KEY,
    tariff_code VARCHAR(20) UNIQUE NOT NULL,
    data JSONB NOT NULL,
    cleaned_at TIMESTAMP NOT NULL,
    source_version INTEGER,
    source_scraped_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_cleaned_data_tariff_code ON cleaned_data(tariff_code);
CREATE INDEX IF NOT EXISTS idx_cleaned_data_cleaned_at ON cleaned_data(cleaned_at);
CREATE INDEX IF NOT EXISTS idx_cleaned_data_jsonb ON cleaned_data USING GIN(data);

-- Comments
COMMENT ON TABLE cleaned_data IS 'Cleaned and normalized scraped data';


