-- Migration: Add hash column for deduplication
-- Run this to add hash-based deduplication to existing database

-- Add hash column if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'scraped_data' AND column_name = 'data_hash'
    ) THEN
        ALTER TABLE scraped_data ADD COLUMN data_hash VARCHAR(64);
        CREATE INDEX IF NOT EXISTS idx_scraped_data_hash ON scraped_data(data_hash);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_scraped_data_tariff_hash ON scraped_data(tariff_code, data_hash);
        
        -- Calculate hashes for existing records (optional - can be done in application)
        -- This is just the schema change
    END IF;
END $$;

COMMENT ON COLUMN scraped_data.data_hash IS 'SHA256 hash of data JSON for deduplication';

