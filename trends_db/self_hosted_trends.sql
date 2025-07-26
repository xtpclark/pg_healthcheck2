-- =================================================================
-- Schema for the Health Check Trend Analysis Platform (Self-Hosted)
-- =================================================================

-- Create the main 'health_check_runs' table
CREATE TABLE IF NOT EXISTS health_check_runs (
    id SERIAL PRIMARY KEY,
    -- For self-hosted, we store the company name directly.
    company_name TEXT NOT NULL,
    run_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    db_technology TEXT NOT NULL,
    target_host TEXT NOT NULL,
    target_port INT NOT NULL,
    target_db_name TEXT NOT NULL,
    -- This column will be populated by a server-side trigger or API
    findings JSONB -- Storing as JSONB for direct query access
);

-- Create a composite index for efficient querying
CREATE INDEX IF NOT EXISTS idx_health_check_runs_metadata
ON health_check_runs (company_name, run_timestamp DESC);

-- =================================================================
-- Grant necessary privileges to the application's runtime user
-- =================================================================
-- Replace 'your_app_user' with the actual user configured in trends.yaml
GRANT CONNECT ON DATABASE postgres TO your_app_user;
GRANT USAGE ON SCHEMA public TO your_app_user;
GRANT INSERT ON health_check_runs TO your_app_user;
GRANT USAGE, SELECT ON SEQUENCE health_check_runs_id_seq TO your_app_user;
