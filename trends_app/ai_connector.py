import requests
import json
import psycopg2
from flask import current_app
from .utils import load_trends_config

def get_ai_recommendation(prompt, profile_id):
    """
    Fetches AI provider and user preference details from the database,
    sends a prompt to the specified AI, and returns the response.
    
    Includes automatic model validation and correction if the configured
    model is not available.

    Args:
        prompt (str): The fully-formed prompt to send to the AI.
        profile_id (int): The ID of the user_ai_profiles entry to use.

    Returns:
        str: The text content of the AI's response, or an error message.
    """
    config = load_trends_config()
    db_config = config.get('database')
    if not db_config:
        return "Error: Database configuration not found."

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Fetch provider details and user preferences
        # Prioritize user-selected model (up.model_name) over provider default (p.api_model)
        query = """
            SELECT
                p.api_endpoint,
                COALESCE(up.model_name, p.api_model) as model_to_use,
                pgp_sym_decrypt(p.encrypted_api_key::bytea, get_encryption_key()),
                pgp_sym_decrypt(up.encrypted_user_api_key::bytea, get_encryption_key()),
                up.proxy_username,
                up.temperature,
                up.max_output_tokens,
                p.id as provider_id,
                p.provider_type
            FROM user_ai_profiles up
            JOIN ai_providers p ON up.provider_id = p.id
            WHERE up.id = %s;
        """
        cursor.execute(query, (profile_id,))
        profile_data = cursor.fetchone()

        if not profile_data:
            return f"Error: AI Profile with ID {profile_id} not found."

        (
            api_endpoint, api_model, system_api_key, user_api_key,
            proxy_username, temperature, max_tokens, provider_id, provider_type
        ) = profile_data

        # Prioritize user's API key, fall back to system key
        api_key_to_use = user_api_key or system_api_key
        if not api_key_to_use:
            return "Error: No valid API key found for the selected AI profile. Please check your user settings or contact an administrator."

        # Validate and potentially auto-correct the model
        api_model = validate_and_correct_model(
            conn, cursor, api_endpoint, api_key_to_use, api_model, 
            profile_id, provider_id, provider_type, config
        )

        if not api_model:
            return "Error: Could not determine a valid model to use."

        # --- Build the API Request ---
        headers = {'Content-Type': 'application/json'}
        
        # Add proxy username header if configured
        if proxy_username and config.get('ai_user_header'):
            headers[config['ai_user_header']] = proxy_username

        # Handle SSL verification
        verify_ssl = config.get('ai_ssl_verify', True)
        ssl_cert_path = config.get('ssl_cert_path')
        if verify_ssl and ssl_cert_path:
            verify_ssl = ssl_cert_path

        # Build provider-specific request
        if "generativelanguage.googleapis.com" in api_endpoint:
            # Google Gemini format
            api_url = f"{api_endpoint}{api_model}:generateContent?key={api_key_to_use}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": float(temperature),
                    "maxOutputTokens": int(max_tokens)
                }
            }
        else:
            # OpenAI-compatible format (OpenAI, xAI, Azure, etc.)
            api_url = f"{api_endpoint}"
            headers['Authorization'] = f'Bearer {api_key_to_use}'
            payload = {
                "model": api_model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": float(temperature),
                "max_tokens": int(max_tokens)
            }
            
            # Add user identification for OpenAI-compatible APIs
            if proxy_username:
                payload["user"] = proxy_username
        
        # Make the API request
        response = requests.post(
            api_url,
            headers=headers,
            data=json.dumps(payload),
            verify=verify_ssl,
            timeout=config.get('ai_timeout', 300)
        )
        response.raise_for_status()
        result = response.json()

        # Parse provider-specific response
        if "generativelanguage.googleapis.com" in api_endpoint:
            return result['candidates'][0]['content']['parts'][0]['text']
        else:
            return result['choices'][0]['message']['content']

    except psycopg2.Error as db_err:
        current_app.logger.error(f"Database error in AI connector: {db_err}")
        return f"Error: A database error occurred while fetching AI configuration."
    except requests.exceptions.SSLError as ssl_err:
        current_app.logger.error(f"SSL error calling AI API: {ssl_err}")
        return f"Error: SSL verification failed. Check ssl_cert_path in config or set ai_ssl_verify: false"
    except requests.exceptions.Timeout as timeout_err:
        current_app.logger.error(f"Timeout calling AI API: {timeout_err}")
        return f"Error: Request to AI service timed out. Consider increasing ai_timeout in config."
    except requests.exceptions.RequestException as http_err:
        current_app.logger.error(f"HTTP error calling AI API: {http_err}")

        # Provide user-friendly error messages for common HTTP status codes
        if hasattr(http_err, 'response') and http_err.response is not None:
            status_code = http_err.response.status_code

            if status_code == 429:
                return (
                    "Error: Rate limit exceeded. You've made too many requests to the AI provider.\n\n"
                    "Solutions:\n"
                    "- Wait a few minutes and try again\n"
                    "- Check your API quota at the provider's dashboard\n"
                    "- For Google Gemini: Visit https://aistudio.google.com/app/apikey to check limits\n"
                    "- Consider upgrading your API plan for higher rate limits\n"
                    "- Reduce the number of runs selected for bulk analysis"
                )
            elif status_code == 401:
                return (
                    "Error: Authentication failed. Your API key is invalid or expired.\n\n"
                    "Solutions:\n"
                    "- Verify your API key at /profile/ai-settings\n"
                    "- Generate a new API key from your AI provider\n"
                    "- Ensure the API key has not expired"
                )
            elif status_code == 403:
                return (
                    "Error: Access forbidden. Your API key doesn't have permission for this operation.\n\n"
                    "Solutions:\n"
                    "- Check if your API key has the required permissions\n"
                    "- Verify the model you selected is available in your plan\n"
                    "- Some models require special access - check provider documentation"
                )
            elif status_code == 404:
                return (
                    "Error: AI service endpoint not found.\n\n"
                    "Solutions:\n"
                    "- Verify the API endpoint URL is correct in AI provider settings\n"
                    "- Check if the model name is correct\n"
                    "- The model may have been deprecated or renamed"
                )
            elif status_code == 500:
                return (
                    "Error: AI provider server error (500).\n\n"
                    "Solutions:\n"
                    "- The AI provider is experiencing issues\n"
                    "- Wait a few minutes and try again\n"
                    "- Check the provider's status page for outages"
                )
            elif status_code == 503:
                return (
                    "Error: AI service temporarily unavailable (503).\n\n"
                    "Solutions:\n"
                    "- The service is temporarily down for maintenance\n"
                    "- Wait a few minutes and try again\n"
                    "- Try a different AI profile if available"
                )
            else:
                return f"Error: Could not connect to the AI service. Status: {status_code}"
        else:
            return "Error: Could not connect to the AI service. No response received."
    except (KeyError, IndexError) as parse_err:
        current_app.logger.error(f"Failed to parse AI response: {parse_err}")
        return f"Error: Received unexpected response format from AI service."
    except Exception as e:
        current_app.logger.error(f"An unexpected error occurred in AI connector: {e}")
        return f"Error: An unexpected error occurred during AI analysis."
    finally:
        if conn:
            conn.close()


def validate_and_correct_model(conn, cursor, api_endpoint, api_key, current_model, 
                               profile_id, provider_id, provider_type, config):
    """
    Validates the current model and auto-corrects if invalid.
    Simplified version without external dependencies.
    """
    try:
        # If we don't have a provider type, we can't validate
        if not provider_type:
            current_app.logger.info(
                f"Provider type unknown for profile {profile_id}. "
                f"Using model '{current_model}' without validation."
            )
            return current_model
        
        # Get cached models for this provider
        cursor.execute("""
            SELECT model_name 
            FROM ai_provider_models 
            WHERE provider_id = %s AND is_available = TRUE
            ORDER BY sort_order;
        """, (provider_id,))
        
        cached_models = [row[0] for row in cursor.fetchall()]
        
        # If model is in cache, it's valid
        if current_model in cached_models:
            return current_model
        
        # If no cached models, try to discover
        if not cached_models:
            from .profile import discover_models_for_provider
            discovered = discover_models_for_provider(provider_type, api_endpoint, api_key, config)
            if discovered:
                cached_models = [m['name'] for m in discovered]
        
        # If still no models, can't validate
        if not cached_models:
            current_app.logger.warning(
                f"No models available for provider {provider_id}. "
                f"Using configured model '{current_model}' without validation."
            )
            return current_model
        
        # Model not found, pick best alternative
        suggested_model = pick_best_model(cached_models, provider_type)
        
        if suggested_model:
            current_app.logger.warning(
                f"Model '{current_model}' not available for profile {profile_id}. "
                f"Auto-correcting to '{suggested_model}'."
            )
            
            # Update the user's profile with corrected model
            cursor.execute(
                "UPDATE user_ai_profiles SET model_name = %s WHERE id = %s;",
                (suggested_model, profile_id)
            )
            conn.commit()
            
            return suggested_model
        
        # If we can't find a better model, stick with current
        return current_model
        
    except Exception as e:
        current_app.logger.error(f"Error validating model: {e}")
        return current_model


def pick_best_model(model_names, provider_type):
    """Pick the best model from available options based on provider type."""
    if not model_names:
        return None
    
    # Provider-specific preferences
    preferences = {
        'google_gemini': ['1.5-flash', 'flash', '1.5-pro', 'pro'],
        'openai': ['gpt-4o', 'gpt-4', 'gpt-3.5'],
        'anthropic': ['sonnet', 'opus', 'haiku'],
        'xai': ['grok-beta'],
        'deepseek': ['deepseek-chat'],
        'together': ['llama', 'mistral'],
        'openrouter': ['gpt-4o', 'claude'],
    }
    
    provider_prefs = preferences.get(provider_type, [])
    
    # Try to find preferred model
    for pref in provider_prefs:
        for name in model_names:
            if pref.lower() in name.lower():
                return name
    
    # Fall back to first available
    return model_names[0]
