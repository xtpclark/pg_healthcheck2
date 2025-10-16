-- Insert Google Gemini
INSERT INTO ai_providers (
    provider_name, 
    api_endpoint, 
    api_model,
    provider_type,
    supports_discovery,
    is_active,
    allow_user_keys
) VALUES (
    'Google Gemini',
    'https://generativelanguage.googleapis.com/v1beta/models/',
    'gemini-1.5-flash',
    'google_gemini',
    TRUE,
    TRUE,
    TRUE
) ON CONFLICT (provider_name) DO NOTHING;

-- Insert OpenAI
INSERT INTO ai_providers (
    provider_name,
    api_endpoint,
    api_model,
    provider_type,
    supports_discovery,
    is_active,
    allow_user_keys
) VALUES (
    'OpenAI',
    'https://api.openai.com/v1/chat/completions',
    'gpt-4o',
    'openai',
    TRUE,
    TRUE,
    TRUE
) ON CONFLICT (provider_name) DO NOTHING;

-- Insert Anthropic Claude
INSERT INTO ai_providers (
    provider_name,
    api_endpoint,
    api_model,
    provider_type,
    supports_discovery,
    is_active,
    allow_user_keys
) VALUES (
    'Anthropic Claude',
    'https://api.anthropic.com/v1/messages',
    'claude-3-5-sonnet-20241022',
    'anthropic',
    FALSE,  -- Anthropic doesn't have a models discovery endpoint
    TRUE,
    TRUE
) ON CONFLICT (provider_name) DO NOTHING;

-- Insert xAI Grok
INSERT INTO ai_providers (
    provider_name,
    api_endpoint,
    api_model,
    provider_type,
    supports_discovery,
    is_active,
    allow_user_keys
) VALUES (
    'xAI Grok',
    'https://api.x.ai/v1/chat/completions',
    'grok-beta',
    'xai',
    TRUE,
    TRUE,
    TRUE
) ON CONFLICT (provider_name) DO NOTHING;

-- Insert Azure OpenAI (requires custom configuration)
INSERT INTO ai_providers (
    provider_name,
    api_endpoint,
    api_model,
    provider_type,
    supports_discovery,
    is_active,
    allow_user_keys
) VALUES (
    'Azure OpenAI',
    'https://YOUR-RESOURCE.openai.azure.com/openai/deployments/YOUR-DEPLOYMENT/chat/completions?api-version=2024-02-01',
    'gpt-4o',
    'azure_openai',
    TRUE,
    FALSE,  -- Disabled by default - requires custom configuration
    TRUE
) ON CONFLICT (provider_name) DO NOTHING;

-- Insert Ollama (local)
INSERT INTO ai_providers (
    provider_name,
    api_endpoint,
    api_model,
    provider_type,
    supports_discovery,
    is_active,
    allow_user_keys
) VALUES (
    'Ollama (Local)',
    'http://localhost:11434/api/chat',
    'llama3',
    'ollama',
    TRUE,
    FALSE,  -- Disabled by default - only enable if running locally
    FALSE   -- No API keys needed for local
) ON CONFLICT (provider_name) DO NOTHING;

-- Insert Together AI
INSERT INTO ai_providers (
    provider_name,
    api_endpoint,
    api_model,
    provider_type,
    supports_discovery,
    is_active,
    allow_user_keys
) VALUES (
    'Together AI',
    'https://api.together.xyz/v1/chat/completions',
    'meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo',
    'together',
    TRUE,
    TRUE,
    TRUE
) ON CONFLICT (provider_name) DO NOTHING;

-- Insert DeepSeek
INSERT INTO ai_providers (
    provider_name,
    api_endpoint,
    api_model,
    provider_type,
    supports_discovery,
    is_active,
    allow_user_keys
) VALUES (
    'DeepSeek',
    'https://api.deepseek.com/v1/chat/completions',
    'deepseek-chat',
    'deepseek',
    TRUE,
    TRUE,
    TRUE
) ON CONFLICT (provider_name) DO NOTHING;

-- Insert OpenRouter (aggregator)
INSERT INTO ai_providers (
    provider_name,
    api_endpoint,
    api_model,
    provider_type,
    supports_discovery,
    is_active,
    allow_user_keys
) VALUES (
    'OpenRouter',
    'https://openrouter.ai/api/v1/chat/completions',
    'openai/gpt-4o',
    'openrouter',
    TRUE,
    TRUE,
    TRUE
) ON CONFLICT (provider_name) DO NOTHING;

-- Seed some default models for Anthropic (since it doesn't support discovery)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, sort_order)
SELECT 
    id,
    'claude-3-5-sonnet-20241022',
    'Claude 3.5 Sonnet',
    'Most intelligent model for complex tasks',
    1
FROM ai_providers 
WHERE provider_name = 'Anthropic Claude'
ON CONFLICT (provider_id, model_name) DO NOTHING;

INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, sort_order)
SELECT 
    id,
    'claude-3-5-haiku-20241022',
    'Claude 3.5 Haiku',
    'Fastest model for simple tasks',
    2
FROM ai_providers 
WHERE provider_name = 'Anthropic Claude'
ON CONFLICT (provider_id, model_name) DO NOTHING;

INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, sort_order)
SELECT 
    id,
    'claude-3-opus-20240229',
    'Claude 3 Opus',
    'Previous generation flagship model',
    3
FROM ai_providers 
WHERE provider_name = 'Anthropic Claude'
ON CONFLICT (provider_id, model_name) DO NOTHING;
