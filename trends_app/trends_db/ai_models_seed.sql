-- ============================================================================
-- AI Provider Models Seed Data
-- Purpose: Populate ai_provider_models with current models suitable for
--          text analysis and report generation
-- Date: 2025-01-15
-- ============================================================================

-- ============================================================================
-- GOOGLE GEMINI MODELS
-- ============================================================================
-- Gemini 2.0 Flash (Latest - December 2024)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gemini-2.0-flash-exp',
    'Gemini 2.0 Flash (Experimental)',
    'Latest experimental model with multimodal capabilities and thinking mode',
    '{"input_token_limit": 1000000, "output_token_limit": 8192}'::jsonb,
    1
FROM ai_providers 
WHERE provider_type = 'google_gemini'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Gemini 1.5 Flash (Recommended for most use cases)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gemini-1.5-flash',
    'Gemini 1.5 Flash',
    'Fast and efficient model, best for most analysis tasks',
    '{"input_token_limit": 1000000, "output_token_limit": 8192}'::jsonb,
    2
FROM ai_providers 
WHERE provider_type = 'google_gemini'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Gemini 1.5 Flash-8B (Smaller, faster)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gemini-1.5-flash-8b',
    'Gemini 1.5 Flash 8B',
    'Smaller, faster variant for simpler analysis tasks',
    '{"input_token_limit": 1000000, "output_token_limit": 8192}'::jsonb,
    3
FROM ai_providers 
WHERE provider_type = 'google_gemini'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Gemini 1.5 Pro (Most capable)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gemini-1.5-pro',
    'Gemini 1.5 Pro',
    'Most capable model for complex analysis and longer documents',
    '{"input_token_limit": 2000000, "output_token_limit": 8192}'::jsonb,
    4
FROM ai_providers 
WHERE provider_type = 'google_gemini'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Gemini 1.0 Pro (Legacy, stable)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gemini-1.0-pro',
    'Gemini 1.0 Pro (Legacy)',
    'Previous generation model, stable and reliable',
    '{"input_token_limit": 30720, "output_token_limit": 2048}'::jsonb,
    5
FROM ai_providers 
WHERE provider_type = 'google_gemini'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- OPENAI MODELS
-- ============================================================================
-- GPT-4o (Latest)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gpt-4o',
    'GPT-4o',
    'Latest and most capable OpenAI model, excellent for analysis',
    '{"input_token_limit": 128000, "output_token_limit": 16384}'::jsonb,
    1
FROM ai_providers 
WHERE provider_type = 'openai'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- GPT-4o Mini (Efficient)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gpt-4o-mini',
    'GPT-4o Mini',
    'Fast and cost-effective, great for most analysis tasks',
    '{"input_token_limit": 128000, "output_token_limit": 16384}'::jsonb,
    2
FROM ai_providers 
WHERE provider_type = 'openai'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- GPT-4 Turbo
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gpt-4-turbo',
    'GPT-4 Turbo',
    'Previous generation flagship, very capable',
    '{"input_token_limit": 128000, "output_token_limit": 4096}'::jsonb,
    3
FROM ai_providers 
WHERE provider_type = 'openai'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- GPT-4
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gpt-4',
    'GPT-4',
    'Original GPT-4, highly capable but slower',
    '{"input_token_limit": 8192, "output_token_limit": 8192}'::jsonb,
    4
FROM ai_providers 
WHERE provider_type = 'openai'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- GPT-3.5 Turbo (Budget option)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gpt-3.5-turbo',
    'GPT-3.5 Turbo',
    'Fast and affordable, good for simpler tasks',
    '{"input_token_limit": 16385, "output_token_limit": 4096}'::jsonb,
    5
FROM ai_providers 
WHERE provider_type = 'openai'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- ANTHROPIC CLAUDE MODELS
-- ============================================================================
-- Claude 3.5 Sonnet (Latest)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'claude-3-5-sonnet-20241022',
    'Claude 3.5 Sonnet',
    'Most intelligent Claude model, excellent for complex analysis',
    '{"input_token_limit": 200000, "output_token_limit": 8192}'::jsonb,
    1
FROM ai_providers 
WHERE provider_type = 'anthropic'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Claude 3.5 Haiku (Fast)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'claude-3-5-haiku-20241022',
    'Claude 3.5 Haiku',
    'Fastest Claude model, great for quick analysis',
    '{"input_token_limit": 200000, "output_token_limit": 8192}'::jsonb,
    2
FROM ai_providers 
WHERE provider_type = 'anthropic'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Claude 3 Opus (Previous flagship)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'claude-3-opus-20240229',
    'Claude 3 Opus',
    'Previous generation flagship, very capable',
    '{"input_token_limit": 200000, "output_token_limit": 4096}'::jsonb,
    3
FROM ai_providers 
WHERE provider_type = 'anthropic'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Claude 3 Sonnet
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'claude-3-sonnet-20240229',
    'Claude 3 Sonnet',
    'Previous generation balanced model',
    '{"input_token_limit": 200000, "output_token_limit": 4096}'::jsonb,
    4
FROM ai_providers 
WHERE provider_type = 'anthropic'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Claude 3 Haiku
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'claude-3-haiku-20240307',
    'Claude 3 Haiku',
    'Previous generation fast model',
    '{"input_token_limit": 200000, "output_token_limit": 4096}'::jsonb,
    5
FROM ai_providers 
WHERE provider_type = 'anthropic'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- XAI GROK MODELS
-- ============================================================================
-- Grok Beta
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'grok-beta',
    'Grok Beta',
    'xAI flagship model with real-time knowledge',
    '{"input_token_limit": 131072, "output_token_limit": 8192}'::jsonb,
    1
FROM ai_providers 
WHERE provider_type = 'xai'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Grok Vision Beta
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'grok-vision-beta',
    'Grok Vision Beta',
    'Multimodal version with vision capabilities',
    '{"input_token_limit": 8192, "output_token_limit": 8192}'::jsonb,
    2
FROM ai_providers 
WHERE provider_type = 'xai'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- DEEPSEEK MODELS
-- ============================================================================
-- DeepSeek Chat
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'deepseek-chat',
    'DeepSeek Chat',
    'General purpose model, good for analysis',
    '{"input_token_limit": 32768, "output_token_limit": 4096}'::jsonb,
    1
FROM ai_providers 
WHERE provider_type = 'deepseek'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- DeepSeek Coder
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'deepseek-coder',
    'DeepSeek Coder',
    'Specialized for code and technical content',
    '{"input_token_limit": 32768, "output_token_limit": 4096}'::jsonb,
    2
FROM ai_providers 
WHERE provider_type = 'deepseek'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- TOGETHER AI MODELS (Popular open-source models)
-- ============================================================================
-- Meta Llama 3.1 405B
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'meta-llama/Meta-Llama-3.1-405B-Instruct-Turbo',
    'Llama 3.1 405B Instruct',
    'Largest Llama model, extremely capable',
    '{"input_token_limit": 130000, "output_token_limit": 4096}'::jsonb,
    1
FROM ai_providers 
WHERE provider_type = 'together'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Meta Llama 3.1 70B
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo',
    'Llama 3.1 70B Instruct',
    'Balanced performance and speed',
    '{"input_token_limit": 130000, "output_token_limit": 4096}'::jsonb,
    2
FROM ai_providers 
WHERE provider_type = 'together'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Meta Llama 3.1 8B
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo',
    'Llama 3.1 8B Instruct',
    'Fast and efficient, good for simpler tasks',
    '{"input_token_limit": 130000, "output_token_limit": 4096}'::jsonb,
    3
FROM ai_providers 
WHERE provider_type = 'together'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Qwen 2.5 72B
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'Qwen/Qwen2.5-72B-Instruct-Turbo',
    'Qwen 2.5 72B Instruct',
    'Strong performance on analytical tasks',
    '{"input_token_limit": 32768, "output_token_limit": 4096}'::jsonb,
    4
FROM ai_providers 
WHERE provider_type = 'together'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Mistral Large
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'mistralai/Mixtral-8x22B-Instruct-v0.1',
    'Mixtral 8x22B Instruct',
    'Mistral flagship mixture-of-experts model',
    '{"input_token_limit": 65536, "output_token_limit": 4096}'::jsonb,
    5
FROM ai_providers 
WHERE provider_type = 'together'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- OPENROUTER MODELS (Aggregator - mix of providers)
-- ============================================================================
-- OpenAI GPT-4o via OpenRouter
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'openai/gpt-4o',
    'GPT-4o (via OpenRouter)',
    'OpenAI latest model through OpenRouter',
    '{"input_token_limit": 128000, "output_token_limit": 16384}'::jsonb,
    1
FROM ai_providers 
WHERE provider_type = 'openrouter'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Anthropic Claude via OpenRouter
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'anthropic/claude-3.5-sonnet',
    'Claude 3.5 Sonnet (via OpenRouter)',
    'Anthropic latest model through OpenRouter',
    '{"input_token_limit": 200000, "output_token_limit": 8192}'::jsonb,
    2
FROM ai_providers 
WHERE provider_type = 'openrouter'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Google Gemini Pro via OpenRouter
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'google/gemini-pro-1.5',
    'Gemini Pro 1.5 (via OpenRouter)',
    'Google Gemini through OpenRouter',
    '{"input_token_limit": 1000000, "output_token_limit": 8192}'::jsonb,
    3
FROM ai_providers 
WHERE provider_type = 'openrouter'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- OLLAMA MODELS (Local - Common models)
-- ============================================================================
-- Llama 3.1
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'llama3.1',
    'Llama 3.1',
    'Meta latest open model, runs locally',
    '{"input_token_limit": 8192, "output_token_limit": 2048}'::jsonb,
    1
FROM ai_providers 
WHERE provider_type = 'ollama'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Mistral
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'mistral',
    'Mistral',
    'Efficient open model, good balance',
    '{"input_token_limit": 8192, "output_token_limit": 2048}'::jsonb,
    2
FROM ai_providers 
WHERE provider_type = 'ollama'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Qwen 2.5
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'qwen2.5',
    'Qwen 2.5',
    'Strong analytical capabilities',
    '{"input_token_limit": 32768, "output_token_limit": 2048}'::jsonb,
    3
FROM ai_providers 
WHERE provider_type = 'ollama'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Gemma 2
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gemma2',
    'Gemma 2',
    'Google open model, efficient',
    '{"input_token_limit": 8192, "output_token_limit": 2048}'::jsonb,
    4
FROM ai_providers 
WHERE provider_type = 'ollama'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- AZURE OPENAI MODELS
-- Note: Azure uses deployment names, these are common deployment patterns
-- ============================================================================
-- GPT-4o deployment
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gpt-4o',
    'GPT-4o',
    'Latest OpenAI model via Azure (deployment name)',
    '{"input_token_limit": 128000, "output_token_limit": 16384}'::jsonb,
    1
FROM ai_providers 
WHERE provider_type = 'azure_openai'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- GPT-4 deployment
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gpt-4',
    'GPT-4',
    'GPT-4 via Azure (common deployment name)',
    '{"input_token_limit": 8192, "output_token_limit": 8192}'::jsonb,
    2
FROM ai_providers 
WHERE provider_type = 'azure_openai'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- GPT-35-turbo deployment
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT 
    id,
    'gpt-35-turbo',
    'GPT-3.5 Turbo',
    'GPT-3.5 via Azure (common deployment name)',
    '{"input_token_limit": 16385, "output_token_limit": 4096}'::jsonb,
    3
FROM ai_providers 
WHERE provider_type = 'azure_openai'
ON CONFLICT (provider_id, model_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- VERIFICATION QUERY
-- ============================================================================
-- Uncomment to see what was inserted:
-- SELECT 
--     ap.provider_name,
--     apm.model_name,
--     apm.display_name,
--     apm.sort_order
-- FROM ai_provider_models apm
-- JOIN ai_providers ap ON apm.provider_id = ap.id
-- ORDER BY ap.provider_name, apm.sort_order;
