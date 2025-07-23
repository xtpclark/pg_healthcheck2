import json
import time
import requests
from .dynamic_prompt_generator import generate_dynamic_prompt

def run_recommendation_enhanced(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Handles the ONLINE AI analysis workflow. It generates a prompt on-the-fly
    using the collected findings and sends it to the AI endpoint.
    """
    adoc_content = ["=== AI-Generated Recommendations\nProvides intelligent, context-aware recommendations based on dynamic analysis of database metrics.\n"]
    
    # --- Step 1: Generate the Dynamic Prompt ---
    try:
        # Generate the prompt and analysis data in one step
        dynamic_analysis = generate_dynamic_prompt(all_structured_findings, settings)
        full_prompt = dynamic_analysis['prompt']
    except Exception as e:
        error_message = f"[ERROR]\n====\nDynamic prompt generation failed: {e}\n====\n"
        adoc_content.append(error_message)
        return "\n".join(adoc_content), {} # Return empty structured data on failure

    # --- Step 2: Make the AI API Call ---
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
            }

        response = requests.post(API_URL, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        result = response.json()

        if "generativelanguage.googleapis.com" in AI_ENDPOINT:
            ai_recommendations = result['candidates'][0]['content']['parts'][0]['text']
        else:
            ai_recommendations = result['choices'][0]['message']['content']
        
        adoc_content.append(ai_recommendations)

    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nFailed to get AI recommendations: {e}\n====\n")

    return "\n".join(adoc_content), {} # This module no longer produces its own structured data
