-- =========================================================
-- 0) Extensions
-- =========================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto; -- digest(), gen_random_uuid()

-- =========================================================
-- 1) sections (PK id, section_code non unique, hash code+label)
-- =========================================================
CREATE TABLE IF NOT EXISTS sections (
  id BIGSERIAL PRIMARY KEY,

  section_code TEXT NOT NULL,
  label TEXT NOT NULL,

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP NOT NULL DEFAULT now(),

  CONSTRAINT chk_section_code_format CHECK (section_code ~ '^[0-9]{2}$')
);

CREATE INDEX IF NOT EXISTS idx_sections_code ON sections(section_code);

-- Optionnel mais recommandé : empêche les doublons exacts (même code + même label)
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_sections_hash ON sections(section_hash);

-- =========================================================
-- 2) chapters (chapitre = 2 chiffres, parent = section_id)
-- =========================================================
CREATE TABLE IF NOT EXISTS chapters (
  id BIGSERIAL PRIMARY KEY,

  section_id BIGINT NOT NULL REFERENCES sections(id) ON DELETE RESTRICT,

  chapter_code TEXT NOT NULL,
  label TEXT NOT NULL,

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP NOT NULL DEFAULT now(),

  CONSTRAINT chk_chapter_code_format CHECK (chapter_code ~ '^[0-9]{2}$')
);

CREATE INDEX IF NOT EXISTS idx_chapters_section_id ON chapters(section_id);
CREATE INDEX IF NOT EXISTS idx_chapters_code ON chapters(chapter_code);

-- Optionnel mais recommandé
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_chapters_hash ON chapters(chapter_hash);

-- =========================================================
-- 3) hs4_nodes (4 chiffres, parent = chapitre )
-- =========================================================
CREATE TABLE IF NOT EXISTS hs4_nodes (
  id BIGSERIAL PRIMARY KEY,

  chapter_id BIGINT NOT NULL REFERENCES chapters(id) ON DELETE RESTRICT,

  hs4 TEXT NOT NULL CHECK (hs4 ~ '^[0-9]{4}$'),
  label TEXT,
  present BOOLEAN NOT NULL DEFAULT TRUE,

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_hs4_chapter_id ON hs4_nodes(chapter_id);
CREATE INDEX IF NOT EXISTS idx_hs4_code ON hs4_nodes(hs4);

-- Optionnel mais recommandé
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_hs4_hash ON hs4_nodes(hs4_hash);

-- =========================================================
-- 4) hs6_nodes (6 chiffres, parent = chapitre )
-- =========================================================
CREATE TABLE IF NOT EXISTS hs6_nodes (
  id BIGSERIAL PRIMARY KEY,

  hs4_id BIGINT NOT NULL REFERENCES hs4_nodes(id) ON DELETE RESTRICT,

  hs6 TEXT NOT NULL CHECK (hs6 ~ '^[0-9]{6}$'),
  label TEXT,
  present BOOLEAN NOT NULL DEFAULT TRUE,

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_hs6_hs4_id ON hs6_nodes(hs4_id);
CREATE INDEX IF NOT EXISTS idx_hs6_code ON hs6_nodes(hs6);

-- Optionnel mais recommandé
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_hs6_hash ON hs6_nodes(hs6_hash);

-- =========================================================
-- 5) hs_products (HS10 + données riches, HS8 intégré)
-- =========================================================
CREATE TABLE IF NOT EXISTS hs_products (
  hs10 TEXT PRIMARY KEY CHECK (hs10 ~ '^[0-9]{10}$'),
  hs6_id BIGINT NOT NULL REFERENCES hs6_nodes(id) ON DELETE RESTRICT,

  -- HS codes dérivés automatiquement
  hs4 TEXT GENERATED ALWAYS AS (substring(hs10 from 1 for 4)) STORED,
  hs6 TEXT GENERATED ALWAYS AS (substring(hs10 from 1 for 6)) STORED,
  hs8 TEXT GENERATED ALWAYS AS (substring(hs10 from 1 for 8)) STORED,
  hs8_label TEXT,

  -- hierarchy labels
  section_label TEXT,
  chapter_label TEXT,
  hs4_label TEXT,
  hs6_label TEXT,

  -- champs utiles
  designation TEXT,
  unit_of_measure TEXT,

  -- colonnes JSONB dédiées (agentic-friendly)
  taxation JSONB NOT NULL DEFAULT '{}'::jsonb,
  documents JSONB NOT NULL DEFAULT '{}'::jsonb,
  agreements JSONB NOT NULL DEFAULT '[]'::jsonb,
  import_duty_history JSONB NOT NULL DEFAULT '[]'::jsonb,
  lineage JSONB NOT NULL DEFAULT '{}'::jsonb,

  -- audit brut (optionnel mais recommandé)
  raw JSONB NOT NULL DEFAULT '{}'::jsonb,

  -- RAG
  canonical_text TEXT,
  canonical_hash TEXT,
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Index “navigation”
CREATE INDEX IF NOT EXISTS idx_products_hs6_id ON hs_products(hs6_id);
CREATE INDEX IF NOT EXISTS idx_products_hs8 ON hs_products(hs8);

-- Index JSONB (agentic SQL)
CREATE INDEX IF NOT EXISTS idx_products_taxation_gin ON hs_products USING GIN (taxation);
CREATE INDEX IF NOT EXISTS idx_products_documents_gin ON hs_products USING GIN (documents);
CREATE INDEX IF NOT EXISTS idx_products_agreements_gin ON hs_products USING GIN (agreements);
CREATE INDEX IF NOT EXISTS idx_products_history_gin ON hs_products USING GIN (import_duty_history);
CREATE INDEX IF NOT EXISTS idx_products_lineage_gin ON hs_products USING GIN (lineage);

-- Dédup / RAG
CREATE INDEX IF NOT EXISTS idx_products_hash ON hs_products(canonical_hash);


-- =========================================================================================================
-- taxation, documents, agreements, legal_and_statistical_texts dans chaque colonne JSONB dédiées 
-- =========================================================================================================


-- =========================================================
-- 6) Chunks RAG 
-- =========================================================
CREATE TABLE IF NOT EXISTS rag_chunks (
  chunk_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  hs10 TEXT NOT NULL REFERENCES hs_products(hs10) ON DELETE CASCADE,

  chunk_index INT NOT NULL,
  topic TEXT NOT NULL,          -- taxation/documents/agreements/history/texts/designation/...
  chunk_text TEXT NOT NULL,
  chunk_hash TEXT NOT NULL,     -- sha256(normalize(chunk_text))

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMP NOT NULL DEFAULT now(),

  CONSTRAINT uq_rag_chunk_hash UNIQUE (chunk_hash)
);

CREATE INDEX IF NOT EXISTS idx_rag_chunks_hs10 ON rag_chunks(hs10);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_topic ON rag_chunks(topic);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_meta_gin ON rag_chunks USING GIN (meta);

-- Optionnel: hybrid search plus tard
-- CREATE INDEX IF NOT EXISTS idx_rag_chunks_fts
--   ON rag_chunks USING GIN (to_tsvector('french', chunk_text));

-- =========================================================
-- 7) taxation (linked to hs_products.hs10)
-- =========================================================
CREATE TABLE IF NOT EXISTS taxation (
  id BIGSERIAL PRIMARY KEY,
  hs10 TEXT NOT NULL REFERENCES hs_products(hs10) ON DELETE CASCADE,

  code TEXT NOT NULL,
  label TEXT,
  raw TEXT,

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_taxation_hs10 ON taxation(hs10);
CREATE INDEX IF NOT EXISTS idx_taxation_code ON taxation(code);

-- =========================================================
-- 8) documents (linked to hs_products.hs10)
-- =========================================================
CREATE TABLE IF NOT EXISTS documents (
  id BIGSERIAL PRIMARY KEY,
  hs10 TEXT NOT NULL REFERENCES hs_products(hs10) ON DELETE CASCADE,

  code TEXT NOT NULL,
  name TEXT NOT NULL,
  issuer TEXT NOT NULL,
  raw TEXT,

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_documents_hs10 ON documents(hs10);
CREATE INDEX IF NOT EXISTS idx_documents_code ON documents(code);

-- =========================================================
-- 9) agreements (linked to hs_products.hs10)
-- =========================================================
CREATE TABLE IF NOT EXISTS agreements (
  id BIGSERIAL PRIMARY KEY,
  hs10 TEXT NOT NULL REFERENCES hs_products(hs10) ON DELETE CASCADE,

  country TEXT NOT NULL,
  liste TEXT DEFAULT '',
  di TEXT DEFAULT '0%',
  tpi TEXT DEFAULT '0%',
  raw TEXT,

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agreements_hs10 ON agreements(hs10);
CREATE INDEX IF NOT EXISTS idx_agreements_country ON agreements(country);

-- =========================================================
-- 10) import_duty_history (linked to hs_products.hs10)
-- =========================================================
CREATE TABLE IF NOT EXISTS import_duty_history (
  id BIGSERIAL PRIMARY KEY,
  hs10 TEXT NOT NULL REFERENCES hs_products(hs10) ON DELETE CASCADE,

  date DATE NOT NULL,
  raw TEXT,

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);

-- =========================================================
-- 11) audit_logs (pipeline observability)
-- =========================================================
CREATE TABLE IF NOT EXISTS audit_logs (
  id BIGSERIAL PRIMARY KEY,

  hs10 TEXT,
  status TEXT NOT NULL,        -- success / failed / partial
  message TEXT,
  duration_ms INTEGER,

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_hs10 ON audit_logs(hs10);
CREATE INDEX IF NOT EXISTS idx_audit_logs_status ON audit_logs(status);

