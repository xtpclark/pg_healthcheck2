-- =====================================================================
-- Table: ai_providers
-- Description: Stores AI provider configurations managed by administrators.
-- This table holds system-wide API keys and endpoint details.
-- =====================================================================
CREATE TABLE ai_providers (
    id SERIAL PRIMARY KEY,
    provider_name TEXT NOT NULL UNIQUE,
    api_endpoint TEXT NOT NULL,
    api_model TEXT NOT NULL,
    
    -- Stores the system-wide API key, encrypted. Can be NULL if only
    -- user-provided keys are allowed for this provider.
    encrypted_api_key TEXT,

    is_active BOOLEAN NOT NULL DEFAULT true,

    -- If true, users can provide their own API key for this provider.
    allow_user_keys BOOLEAN NOT NULL DEFAULT false,

    -- Timestamps for auditing
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add comments for clarity
COMMENT ON TABLE ai_providers IS 'Stores AI provider configurations managed by administrators.';
COMMENT ON COLUMN ai_providers.encrypted_api_key IS 'System-wide API key, encrypted using pgcrypto.';
COMMENT ON COLUMN ai_providers.allow_user_keys IS 'If true, users can override the system key with their own.';

-- Trigger to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION set_updated_at_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_ai_providers_updated_at
BEFORE UPDATE ON ai_providers
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();
