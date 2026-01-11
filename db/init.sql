-- init.sql
CREATE TABLE IF NOT EXISTS sections (
    id SERIAL PRIMARY KEY,
    section_code VARCHAR(50) NOT NULL,
    section_hash TEXT GENERATED ALWAYS AS (md5(section_code)) STORED UNIQUE,
    label TEXT,
    meta JSONB DEFAULT '{}',
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chapters (
    id SERIAL PRIMARY KEY,
    section_id INTEGER REFERENCES sections(id),
    chapter_code VARCHAR(50) NOT NULL,
    chapter_hash TEXT GENERATED ALWAYS AS (md5(chapter_code)) STORED UNIQUE,
    label TEXT,
    meta JSONB DEFAULT '{}',
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hs4_nodes (
    id SERIAL PRIMARY KEY,
    chapter_id INTEGER REFERENCES chapters(id),
    hs4 VARCHAR(50) NOT NULL,
    hs4_hash TEXT GENERATED ALWAYS AS (md5(hs4)) STORED UNIQUE,
    label TEXT,
    present BOOLEAN DEFAULT TRUE,
    meta JSONB DEFAULT '{}',
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hs6_nodes (
    id SERIAL PRIMARY KEY,
    hs4_id INTEGER REFERENCES hs4_nodes(id),
    hs6 VARCHAR(50) NOT NULL,
    hs6_hash TEXT GENERATED ALWAYS AS (md5(hs6)) STORED UNIQUE,
    label TEXT,
    present BOOLEAN DEFAULT TRUE,
    meta JSONB DEFAULT '{}',
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hs_products (
    hs10 VARCHAR(50) PRIMARY KEY,
    hs6_id INTEGER REFERENCES hs6_nodes(id),
    designation TEXT,
    unit_of_measure VARCHAR(50),
    entry_into_force_date DATE,
    taxation JSONB,
    documents JSONB,
    agreements JSONB,
    import_duty_history JSONB,
    lineage JSONB,
    raw JSONB,
    updated_at TIMESTAMP DEFAULT NOW()
);
