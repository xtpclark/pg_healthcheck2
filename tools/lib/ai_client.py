# lib/ai_client.py
"""AI API client with multi-provider support."""
import requests
import json
import time
from typing import Optional

def get_ssl_verification(settings):
    """
    Determines SSL verification settings.
    
    Priority:
    1. User-specified cert path
    2. certifi package
    3. System default (True/False from config)
    
    Args:
        settings: Config dict
        
    Returns:
        bool or str: True/False for verify, or path to cert bundle
    """
    ssl_verify = settings.get('ai_ssl_verify', True)
    ssl_cert_path = settings.get('ssl_cert_path', '')
    
    if not ssl_verify:
        return False
    
    if ssl_cert_path:
        return ssl_cert_path
    
    # Try certifi
    try:
        import certifi
        return certifi.where()
    except ImportError:
        pass
    
    # Fall back to system default
    return True

def build_request_payload(prompt, settings, ai_provider, model_override=None):
    """
    Builds provider-specific API request payload.
    
    Args:
        prompt: The prompt text
        settings: Config dict
        ai_provider: Provider name
        model_override: Optional model override
        
    Returns:
        tuple: (url, headers, payload)
    """
    API_ENDPOINT = settings.get('ai_endpoint')
    AI_MODEL = model_override or settings.get('ai_model')
    API_KEY = settings.get('ai_api_key', '')
    AI_TEMPERATURE = settings.get('ai_temperature', 0.7)
    AI_MAX_OUTPUT_TOKENS = settings.get('ai_max_output_tokens', 8192)
    AI_USER = settings.get('ai_user', 'anonymous')
    AI_USER_HEADER = settings.get('ai_user_header', '')
    
    headers = {'Content-Type': 'application/json'}
    
    # Add custom user header if configured
    if AI_USER_HEADER:
        headers[AI_USER_HEADER] = AI_USER
    
    # Provider-specific payload
    if "generativelanguage.googleapis.com" in API_ENDPOINT:
        # Google Gemini
        url = f"{API_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": AI_TEMPERATURE,
                "maxOutputTokens": AI_MAX_OUTPUT_TOKENS
            }
        }
    else:
        # OpenAI-compatible (OpenAI, xAI, etc.)
        url = f"{API_ENDPOINT}/v1/chat/completions"
        headers['Authorization'] = f'Bearer {API_KEY}'
        payload = {
            "model": AI_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": AI_TEMPERATURE,
            "max_tokens": AI_MAX_OUTPUT_TOKENS,
            "user": AI_USER
        }
    
    return url, headers, payload

def parse_ai_response(response_json, ai_provider):
    """
    Parses provider-specific response format.
    
    Args:
        response_json: Response JSON
        ai_provider: Provider name
        
    Returns:
        str: Extracted text response
    """
    if "generativelanguage.googleapis.com" in ai_provider:
        return response_json['candidates'][0]['content']['parts'][0]['text']
    else:
        return response_json['choices'][0]['message']['content']

def execute_ai_prompt(prompt, settings, model_override=None):
    """
    Sends prompt to configured AI service.
    
    Args:
        prompt: The prompt string
        settings: Config dict
        model_override: Optional model name override
        
    Returns:
        str: AI response text, or None on error
    """
    if not settings:
        print("❌ AI settings not loaded.")
        return None

    ai_provider = settings.get('ai_provider', 'openai')
    AI_MODEL = model_override or settings.get('ai_model')
    API_KEY = settings.get('ai_api_key', '')
    API_ENDPOINT = settings.get('ai_endpoint')

    # Validation
    if not API_KEY:
        print("❌ AI API key not found in config.yaml")
        return None
    if not API_ENDPOINT:
        print("❌ AI endpoint not configured in config.yaml")
        return None
    if not AI_MODEL:
        print("❌ AI model not configured in config.yaml")
        return None

    print(f"  - Contacting AI ({AI_MODEL})...")

    try:
        # Build request
        url, headers, payload = build_request_payload(
            prompt, settings, ai_provider, model_override
        )
        
        # Get SSL verification settings
        verify_ssl = get_ssl_verification(settings)
        
        # Make request
        start_time = time.time()
        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(payload),
            verify=verify_ssl,
            timeout=settings.get('ai_timeout', 300)
        )
        response.raise_for_status()
        result = response.json()
        
        duration = time.time() - start_time
        print(f"  - AI Processing Time: {duration:.2f} seconds")
        
        # Parse response
        return parse_ai_response(result, API_ENDPOINT)

    except requests.exceptions.SSLError as e:
        print(f"❌ SSL Error: {e}")
        print(f"   Check ssl_cert_path in config.yaml or set ai_ssl_verify: false")
        return None
    except requests.exceptions.Timeout as e:
        print(f"❌ Request Timeout: {e}")
        print(f"   Consider increasing ai_timeout in config.yaml")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ API Request Failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response Status: {e.response.status_code}")
            print(f"   Response Body: {e.response.text[:200]}")
        return None
    except (KeyError, IndexError) as e:
        print(f"❌ Failed to parse AI response: {e}")
        print(f"   Provider: {ai_provider}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error during AI request: {e}")
        return None
