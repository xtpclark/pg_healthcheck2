-- ============================================================================
-- AI Provider Models Seed Data
-- Purpose: Populate ai_provider_models with current models suitable for
--          code conversion, analysis, and report generation.
-- Date: 2025-10-15
-- Note: This list has been curated to remove legacy models and focus on
--       options with large context windows and strong reasoning capabilities.
-- ============================================================================

-- ============================================================================
-- GOOGLE GEMINI MODELS
-- Note: Based on your available API models. Prioritizing the latest stable
--       2.5 series for their massive context windows and reasoning.
-- ============================================================================
-- Gemini 2.5 Pro (Recommended Flagship)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'gemini-2.5-pro',
    'Gemini 2.5 Pro',
    'Flagship model for complex reasoning, analysis, and large codebases. Best for high-fidelity tasks.',
    '{"input_token_limit": 1048576, "output_token_limit": 65536}'::jsonb,
    1
FROM ai_providers
WHERE provider_type = 'google_gemini'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Gemini 2.5 Flash (Fast & Capable)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'gemini-2.5-flash',
    'Gemini 2.5 Flash',
    'Fast and cost-effective model, excellent for most analysis and iterative debugging tasks.',
    '{"input_token_limit": 1048576, "output_token_limit": 65536}'::jsonb,
    2
FROM ai_providers
WHERE provider_type = 'google_gemini'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- OPENAI MODELS
-- Note: Focusing on the latest 'o' series. GPT-4 and older are removed.
-- ============================================================================
-- GPT-4o (Flagship)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'gpt-4o',
    'GPT-4o',
    'Latest and most capable OpenAI model, excellent for analysis and complex code generation.',
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
    'Fast and cost-effective, great for iterative tasks where speed is important.',
    '{"input_token_limit": 128000, "output_token_limit": 16384}'::jsonb,
    2
FROM ai_providers
WHERE provider_type = 'openai'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- ANTHROPIC CLAUDE MODELS
-- Note: Focusing on the latest 3.5 series. The older Claude 3 models are removed.
-- ============================================================================
-- Claude 3.5 Sonnet (Flagship)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'claude-3.5-sonnet-20240620',
    'Claude 3.5 Sonnet',
    'Most intelligent Claude model, industry-leading for complex analysis and large documents.',
    '{"input_token_limit": 200000, "output_token_limit": 8192}'::jsonb,
    1
FROM ai_providers
WHERE provider_type = 'anthropic'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Claude 3.5 Haiku (Fastest)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'claude-3.5-haiku-20240620',
    'Claude 3.5 Haiku',
    'Fastest Claude model, excellent for real-time interaction and iterative tasks.',
    '{"input_token_limit": 200000, "output_token_limit": 8192}'::jsonb,
    2
FROM ai_providers
WHERE provider_type = 'anthropic'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- XAI GROK MODELS
-- Note: Including the latest Grok models as requested for their strong reasoning capabilities.
-- ============================================================================
-- Grok 3 (Flagship)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'grok-3',
    'Grok 3',
    'xAI''s latest flagship model with a very large context window, excellent for complex reasoning and analysis.',
    '{"input_token_limit": 128000, "output_token_limit": 16384}'::jsonb,
    1
FROM ai_providers
WHERE provider_type = 'xai'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Grok 3 Vision (Multimodal)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'grok-3-vision',
    'Grok 3 Vision',
    'Multimodal version of Grok 3, capable of processing and analyzing visual information like diagrams and screenshots.',
    '{"input_token_limit": 128000, "output_token_limit": 16384}'::jsonb,
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
-- Note: Specialized coding models are ideal for your use case.
-- ============================================================================
-- DeepSeek V2 Coder
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'deepseek-v2-coder',
    'DeepSeek V2 Coder',
    'Specialized for code generation, conversion, and analysis. Highly recommended for technical tasks.',
    '{"input_token_limit": 128000, "output_token_limit": 4096}'::jsonb,
    1
FROM ai_providers
WHERE provider_type = 'deepseek'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- TOGETHER AI MODELS (Popular open-source models)
-- Note: Curated to top performers for code and logic.
-- ============================================================================
-- Meta Llama 3.1 405B
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'meta-llama/Meta-Llama-3.1-405B-Instruct-Turbo',
    'Llama 3.1 405B Instruct',
    'Largest open-source model, extremely capable for complex reasoning and code.',
    '{"input_token_limit": 131072, "output_token_limit": 8192}'::jsonb,
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
    'Excellent balance of performance and speed for most coding tasks.',
    '{"input_token_limit": 131072, "output_token_limit": 8192}'::jsonb,
    2
FROM ai_providers
WHERE provider_type = 'together'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- Mixtral 8x22B
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'mistralai/Mixtral-8x22B-Instruct-v0.1',
    'Mixtral 8x22B Instruct',
    'Mistral flagship mixture-of-experts model, strong general-purpose reasoner.',
    '{"input_token_limit": 65536, "output_token_limit": 4096}'::jsonb,
    3
FROM ai_providers
WHERE provider_type = 'together'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- OLLAMA MODELS (Local - Common models for code)
-- Note: Token limits depend on local configuration but provided reasonable defaults.
-- ============================================================================
-- Llama 3.1 70B (Recommended Local)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'llama3.1:70b',
    'Llama 3.1 70B',
    'Meta latest open model (70B), top-tier for local code generation.',
    '{"input_token_limit": 8192, "output_token_limit": 4096}'::jsonb,
    1
FROM ai_providers
WHERE provider_type = 'ollama'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- DeepSeek Coder V2 (Specialized Local)
INSERT INTO ai_provider_models (provider_id, model_name, display_name, description, capabilities, sort_order)
SELECT
    id,
    'deepseek-coder-v2',
    'DeepSeek Coder V2',
    'Specialized code model from DeepSeek, runs locally.',
    '{"input_token_limit": 16384, "output_token_limit": 4096}'::jsonb,
    2
FROM ai_providers
WHERE provider_type = 'ollama'
ON CONFLICT (provider_id, model_name) DO UPDATE
SET display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    capabilities = EXCLUDED.capabilities,
    sort_order = EXCLUDED.sort_order;

-- ============================================================================
-- OTHER PROVIDERS (Removed for Brevity or Redundancy)
-- Note:
-- - OpenRouter/Together: Many models are already listed under primary providers.
-- - Azure: The model names are identical to OpenAI ('gpt-4o').
-- ============================================================================


-- ============================================================================
-- VERIFICATION QUERY
-- ============================================================================
-- Uncomment to see what was inserted:
/*
SELECT
    ap.provider_name,
    apm.model_name,
    apm.display_name,
    apm.sort_order,
    apm.capabilities
FROM ai_provider_models apm
JOIN ai_providers ap ON apm.provider_id = ap.id
ORDER BY ap.provider_name, apm.sort_order;
*/
