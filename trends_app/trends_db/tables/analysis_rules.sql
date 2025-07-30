-- =====================================================================
-- Table: analysis_rules
-- Description: Stores different sets of analysis rules for generating
-- dynamic AI prompts. This allows for different analysis "modes".
-- =====================================================================
CREATE TABLE analysis_rules (
    id SERIAL PRIMARY KEY,
    rule_set_name TEXT NOT NULL UNIQUE,
    technology TEXT NOT NULL, -- e.g., 'postgres', 'mysql'
    
    -- The entire rule set (from analysis_rules.py) stored as a single JSONB object.
    rules_json JSONB NOT NULL,

    -- Timestamps for auditing
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add comments for clarity
COMMENT ON TABLE analysis_rules IS 'Stores sets of rules used by the prompt generator to analyze health check findings.';
COMMENT ON COLUMN analysis_rules.rules_json IS 'The complete analysis rule configuration in JSONB format.';

-- Re-use the timestamp trigger function
CREATE TRIGGER set_analysis_rules_updated_at
BEFORE UPDATE ON analysis_rules
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();
