-- =====================================================================
-- Table: prompt_templates
-- Description: Stores Jinja2 templates for generating audience-specific
-- AI analysis prompts.
-- =====================================================================
CREATE TABLE prompt_templates (
    id SERIAL PRIMARY KEY,
    template_name TEXT NOT NULL UNIQUE,
    technology TEXT NOT NULL, -- e.g., 'postgres', 'mysql'
    
    -- The full content of the Jinja2 template file.
    template_content TEXT NOT NULL,

    -- Timestamps for auditing
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add comments for clarity
COMMENT ON TABLE prompt_templates IS 'Stores Jinja2 templates for generating AI prompts tailored to different audiences.';
COMMENT ON COLUMN prompt_templates.template_content IS 'The complete content of the .j2 template file.';

-- Re-use the timestamp trigger function if it exists from previous tables
-- CREATE TRIGGER set_prompt_templates_updated_at ...
