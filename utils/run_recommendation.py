import json
import requests

def run_recommendation(settings, full_prompt):
    """
    Handles the AI analysis workflow by sending a pre-generated prompt to the AI endpoint.
    This function is technology-agnostic.
    """
    adoc_content = ["=== AI-Generated Recommendations\nProvides intelligent, context-aware recommendations based on dynamic analysis of database metrics.\n"]
    
    # This logic is only for the online/integrated run.
    if not settings.get('ai_run_integrated', True):
        adoc_content.append("[NOTE]\n====\nOnline AI analysis is disabled (`ai_run_integrated: false`). Use the offline_ai_processor.py to generate reports from the saved findings.\n====\n")
        return "\n".join(adoc_content), {}

    API_KEY = settings.get('ai_api_key', '')
    if not API_KEY:
        adoc_content.append("[ERROR]\n====\nAI API key not found in config.yaml. Cannot perform online analysis.\n====\n")
        return "\n".join(adoc_content), {}

    try:
        AI_ENDPOINT = settings.get('ai_endpoint')
        AI_MODEL = settings.get('ai_model')
        AI_TEMPERATURE = settings.get('ai_temperature', 0.7)
        AI_MAX_OUTPUT_TOKENS = settings.get('ai_max_output_tokens', 2048)
        headers = {'Content-Type': 'application/json'}
        
        # SSL and User settings for proxies
        AI_USER = settings.get('ai_user', 'anonymous')
        AI_USER_HEADER = settings.get('ai_user_header', '')
        SSL_CERT_PATH = settings.get('ssl_cert_path', '')
        AI_SSL_VERIFY = settings.get('ai_ssl_verify', True)
        
        if AI_USER_HEADER:
            headers[AI_USER_HEADER] = AI_USER

        verify_ssl = AI_SSL_VERIFY
        if verify_ssl and SSL_CERT_PATH:
            verify_ssl = SSL_CERT_PATH
            
        if "generativelanguage.googleapis.com" in AI_ENDPOINT:
            API_URL = f"{AI_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}"
            payload = {
                "contents": [{"parts": [{"text": full_prompt}]}],
                "generationConfig": {
                    "temperature": AI_TEMPERATURE,
                    "maxOutputTokens": AI_MAX_OUTPUT_TOKENS
                }
            }
        else:
            API_URL = f"{AI_ENDPOINT}v1/chat/completions"
            headers['Authorization'] = f'Bearer {API_KEY}'
            payload = {
                "model": AI_MODEL,
                "messages": [{"role": "user", "content": full_prompt}],
                "temperature": AI_TEMPERATURE,
                "max_tokens": AI_MAX_OUTPUT_TOKENS
                "user" : AI_USER
            }

        response = requests.post(API_URL, headers=headers, data=json.dumps(payload), verify=verify_ssl)
        response.raise_for_status()
        result = response.json()

        if "generativelanguage.googleapis.com" in AI_ENDPOINT:
            ai_recommendations = result['candidates'][0]['content']['parts'][0]['text']
        else:
            ai_recommendations = result['choices'][0]['message']['content']
        
        adoc_content.append(ai_recommendations)

    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nFailed to get AI recommendations: {e}\n====\n")

    # This module no longer produces its own structured data in the new architecture
    return "\n".join(adoc_content), {}
