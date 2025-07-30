-- =====================================================================
-- Table: generated_ai_reports
-- Description: Stores the encrypted output of AI analysis runs, along
-- with user-provided metadata and annotations.
-- =====================================================================
CREATE TABLE generated_ai_reports (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL,
    rule_set_id INTEGER, -- Nullable, as a default might be used
    ai_profile_id INTEGER NOT NULL,
    generated_by_user_id INTEGER NOT NULL,
    generation_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- User-provided metadata
    report_name TEXT,
    report_description TEXT,
    annotations TEXT, -- For ongoing notes and comments

    -- The encrypted Asciidoc content of the report
    report_content TEXT NOT NULL,

    -- Constraints
    CONSTRAINT fk_run
        FOREIGN KEY(run_id) 
        REFERENCES health_check_runs(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_rule_set
        FOREIGN KEY(rule_set_id) 
        REFERENCES analysis_rules(id)
        ON DELETE SET NULL,

    CONSTRAINT fk_ai_profile
        FOREIGN KEY(ai_profile_id) 
        REFERENCES user_ai_profiles(id)
        ON DELETE SET NULL, -- Don't delete the report if the profile is deleted

    CONSTRAINT fk_user
        FOREIGN KEY(generated_by_user_id) 
        REFERENCES users(id)
        ON DELETE SET NULL -- Keep the report even if the generating user is deleted
);

-- Add comments for clarity
COMMENT ON TABLE generated_ai_reports IS 'Stores encrypted AI-generated reports and their associated metadata.';
COMMENT ON COLUMN generated_ai_reports.report_content IS 'The Asciidoc report text, encrypted using pgcrypto.';
