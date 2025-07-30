-- =====================================================================
-- Table: user_ai_profiles
-- Description: Stores user-defined AI configuration profiles. Each user
-- can have multiple profiles for different analysis tasks.
-- =====================================================================
CREATE TABLE user_ai_profiles (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    profile_name TEXT NOT NULL,
    provider_id INTEGER NOT NULL,
    
    -- User-configurable parameters
    temperature NUMERIC(3, 2) NOT NULL DEFAULT 0.7,
    max_output_tokens INTEGER NOT NULL DEFAULT 2048,
    
    -- User-specific credentials (optional)
    encrypted_user_api_key TEXT,
    proxy_username TEXT,

    -- Timestamps for auditing
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT fk_user
        FOREIGN KEY(user_id) 
        REFERENCES users(id)
        ON DELETE CASCADE,
    
    CONSTRAINT fk_provider
        FOREIGN KEY(provider_id) 
        REFERENCES ai_providers(id)
        ON DELETE CASCADE,

    -- A user cannot have two profiles with the same name
    UNIQUE (user_id, profile_name)
);

-- Add comments for clarity
COMMENT ON TABLE user_ai_profiles IS 'Stores user-created profiles for AI analysis, including personal keys and parameters.';
COMMENT ON COLUMN user_ai_profiles.encrypted_user_api_key IS 'User-provided API key, encrypted using pgcrypto.';

-- Re-use the timestamp trigger function
CREATE TRIGGER set_user_ai_profiles_updated_at
BEFORE UPDATE ON user_ai_profiles
FOR EACH ROW
EXECUTE FUNCTION set_updated_at_timestamp();
