import requests
import json
import psycopg2
from flask import current_app
from .utils import load_trends_config

def get_ai_recommendation(prompt, profile_id):
    """
    Fetches AI provider and user preference details from the database,
    sends a prompt to the specified AI, and returns the response.

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

        # This query securely fetches the provider details and decrypts the
        # necessary API keys using the database's get_encryption_key() function.
        # TODO: use a different key for this.  get_encryption_key() key encrypts findings mostly, but this should be different key.
        query = """
            SELECT
                p.api_endpoint,
                p.api_model,
                pgp_sym_decrypt(p.encrypted_api_key::bytea, get_encryption_key()),
                pgp_sym_decrypt(up.encrypted_user_api_key::bytea, get_encryption_key()),
                up.proxy_username,
                up.temperature,
                up.max_output_tokens
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
            proxy_username, temperature, max_tokens
        ) = profile_data

        # Prioritize user's API key, fall back to system key.
        api_key_to_use = user_api_key or system_api_key
        if not api_key_to_use:
            return "Error: No valid API key found for the selected AI profile. Please check your user settings or contact an administrator."

        # --- Make the API Call (logic adapted from run_recommendation.py) ---
        headers = {'Content-Type': 'application/json'}
        
        # This assumes the proxy username would be passed in a specific header.
        # This might need adjustment based on the actual proxy's requirements.
        if proxy_username and config.get('ai_user_header'):
             headers[config['ai_user_header']] = proxy_username

        if "generativelanguage.googleapis.com" in api_endpoint:
            api_url = f"{api_endpoint}{api_model}:generateContent?key={api_key_to_use}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": float(temperature),
                    "maxOutputTokens": int(max_tokens)
                }
            }
        else: # Assuming OpenAI-compatible API
            api_url = f"{api_endpoint}"
            headers['Authorization'] = f'Bearer {api_key_to_use}'
            payload = {
                "model": api_model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": float(temperature),
                "max_tokens": int(max_tokens)
            }
        
        response = requests.post(api_url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        result = response.json()

        if "generativelanguage.googleapis.com" in api_endpoint:
            return result['candidates'][0]['content']['parts'][0]['text']
        else:
            return result['choices'][0]['message']['content']

    except psycopg2.Error as db_err:
        current_app.logger.error(f"Database error in AI connector: {db_err}")
        return f"Error: A database error occurred while fetching AI configuration."
    except requests.exceptions.RequestException as http_err:
        current_app.logger.error(f"HTTP error calling AI API: {http_err}")
        return f"Error: Could not connect to the AI service. Details: {http_err}"
    except Exception as e:
        current_app.logger.error(f"An unexpected error occurred in AI connector: {e}")
        return f"Error: An unexpected error occurred during AI analysis."
    finally:
        if conn:
            conn.close()
