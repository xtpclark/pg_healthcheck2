-- Technology types reference table
-- Provides dynamic dropdown population for technology filters
-- Following pb database *type table pattern

CREATE TABLE IF NOT EXISTS techtype (
    techtype_id      SERIAL PRIMARY KEY,
    techtype_code    TEXT NOT NULL UNIQUE CHECK (techtype_code <> ''),
    techtype_descrip TEXT NOT NULL,
    techtype_order   INTEGER NOT NULL DEFAULT 999,
    techtype_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    techtype_notes   TEXT
);

-- Create index for ordering
CREATE INDEX IF NOT EXISTS idx_techtype_order ON techtype (techtype_order);

-- Comments for documentation
COMMENT ON TABLE techtype IS 'Technology types for health check system. Used to populate dropdown filters and validate db_technology values.';
COMMENT ON COLUMN techtype.techtype_code IS 'Internal code used in database (matches health_check_runs.db_technology)';
COMMENT ON COLUMN techtype.techtype_descrip IS 'Human-readable display name for UI';
COMMENT ON COLUMN techtype.techtype_order IS 'Display order in dropdowns (lower = earlier)';
COMMENT ON COLUMN techtype.techtype_enabled IS 'If false, hidden from dropdown but still valid in database';
COMMENT ON COLUMN techtype.techtype_notes IS 'Optional notes/documentation about the technology';

-- Seed with current technologies
INSERT INTO techtype (techtype_code, techtype_descrip, techtype_order, techtype_enabled, techtype_notes) VALUES
    ('postgres',    'PostgreSQL',  10, TRUE, 'PostgreSQL relational database'),
    ('kafka',       'Kafka',       20, TRUE, 'Apache Kafka streaming platform'),
    ('cassandra',   'Cassandra',   30, TRUE, 'Apache Cassandra NoSQL database'),
    ('opensearch',  'OpenSearch',  40, TRUE, 'OpenSearch search and analytics'),
    ('clickhouse',  'ClickHouse',  50, TRUE, 'ClickHouse columnar database'),
    ('valkey',      'Valkey',      60, TRUE, 'Valkey (Redis fork) in-memory database'),
    ('mysql',       'MySQL',       70, TRUE, 'MySQL relational database'),
    ('mongodb',     'MongoDB',     80, TRUE, 'MongoDB document database')
ON CONFLICT (techtype_code) DO NOTHING;
