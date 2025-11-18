-- Filter Persistence Table (Modern PostgreSQL with JSONB)
-- Stores user-specific filter configurations per screen

CREATE TABLE IF NOT EXISTS filter (
    filter_id SERIAL PRIMARY KEY,
    filter_screen TEXT NOT NULL,           -- e.g., 'dashboard', 'trend_analysis', 'migration_candidates'
    filter_values JSONB NOT NULL,          -- Filter key-value pairs as JSONB
    filter_username TEXT,                  -- NULL = shared/global filter, username = user-specific
    filter_name TEXT NOT NULL,             -- Display name for saved filter
    filter_selected BOOLEAN DEFAULT false, -- Is this the currently selected filter for this user/screen?
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_filter_screen_username ON filter(filter_screen, filter_username);
CREATE INDEX IF NOT EXISTS idx_filter_selected ON filter(filter_screen, filter_username, filter_selected) WHERE filter_selected = true;
CREATE INDEX IF NOT EXISTS idx_filter_values_gin ON filter USING gin(filter_values);  -- For JSONB queries

-- Constraint: Only one selected filter per user per screen
CREATE UNIQUE INDEX IF NOT EXISTS idx_filter_one_selected_per_user_screen
    ON filter(filter_screen, COALESCE(filter_username, ''))
    WHERE filter_selected = true;

COMMENT ON TABLE filter IS 'Stores user-specific filter configurations with JSONB for flexibility';
COMMENT ON COLUMN filter.filter_screen IS 'Screen identifier (e.g., dashboard, trend_analysis)';
COMMENT ON COLUMN filter.filter_values IS 'JSONB object with filter key-value pairs: {"target": "kafka-prod", "timerange": "30d", "status": "critical"}';
COMMENT ON COLUMN filter.filter_username IS 'Username for user-specific filters, NULL for shared/global filters';
COMMENT ON COLUMN filter.filter_name IS 'User-friendly name for the filter preset';
COMMENT ON COLUMN filter.filter_selected IS 'Whether this filter is currently active for the user on this screen';

-- Example filter_values JSONB structure:
-- {
--   "target": "company1:kafka-prod-01:9092",
--   "timerange": "30d",
--   "technology": "kafka",
--   "status": "critical",
--   "search": "memory",
--   "favorites_only": true
-- }
