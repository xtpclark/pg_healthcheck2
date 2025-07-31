"""Handles the interaction with external Generative AI APIs.

This module contains the function responsible for taking a finalized prompt,
constructing the appropriate API request for the configured AI provider
(e.g., OpenAI, Google), sending the request, and parsing the response.
"""

import json
import requests
import time

def run_recommendation(settings, full_prompt):
    """Handles the AI analysis workflow by sending a prompt to an AI service.

    This function reads AI provider details from the settings, builds a
    provider-specific payload (supporting OpenAI-compatible and Google
    Generative AI endpoints), and sends the prompt via an HTTP POST request.
    It times the request, collects execution metrics, and formats the AI's
    response and the metrics into a single AsciiDoc block for inclusion in
    the final report.

    Args:
        settings (dict): The main application settings dictionary, containing all
            AI provider details like `ai_provider`, `ai_endpoint`, `ai_model`,
            `ai_api_key`, etc.
        full_prompt (str): The complete, formatted prompt string to be sent
            to the AI model for analysis.

    Returns:
        tuple[str, dict]: A tuple where the first element is the formatted
        AsciiDoc string containing the AI's response and a details table,
        and the second is a dictionary of execution metrics.
    """

    ai_provider = settings.get('ai_provider', 'openai')
    API_ENDPOINT = settings.get('ai_endpoint')
    AI_MODEL = settings.get('ai_model')
    prompt_chars = len(full_prompt)
    estimated_tokens = prompt_chars // 4


    # Initialize the metrics dictionary that will be returned
    ai_metrics = {
        'ai_provider': ai_provider,
        'ai_model': AI_MODEL,
        'prompt_characters': prompt_chars,
        'prompt_estimated_tokens': estimated_tokens,
        'processing_time_seconds': 0.0
    }

    
    # --- NEW: Create a dedicated AsciiDoc block for AI details ---
    ai_details_adoc = [
        "==== AI Analysis Details",
        "[options=\"header\"]",
        "|===",
        "| Parameter | Value",
        f"| AI Provider | {ai_provider}",
        f"| AI Model | {AI_MODEL}",
        f"| Prompt Size | {prompt_chars:,} characters (~{estimated_tokens:,} tokens)",
    ]

    adoc_content = ["=== AI-Generated Recommendations\nProvides intelligent, context-aware recommendations based on dynamic analysis of database metrics.\n"]

    if not settings.get('ai_run_integrated', True):
        adoc_content.append("[NOTE]\n====\nOnline AI analysis is disabled (`ai_run_integrated: false`).\n====\n")
        return "\n".join(adoc_content), ai_metrics

    API_KEY = settings.get('ai_api_key', '')
    if not API_KEY:
        adoc_content.append("[ERROR]\n====\nAI API key not found in config.yaml. Cannot perform online analysis.\n====\n")
        return "\n".join(adoc_content), 0.0

    try:
        AI_TEMPERATURE = settings.get('ai_temperature', 0.7)
        AI_MAX_OUTPUT_TOKENS = settings.get('ai_max_output_tokens', 2048)
        headers = {'Content-Type': 'application/json'}
        
        AI_USER = settings.get('ai_user', 'anonymous')
        AI_USER_HEADER = settings.get('ai_user_header', '')
        SSL_CERT_PATH = settings.get('ssl_cert_path', '')
        AI_SSL_VERIFY = settings.get('ai_ssl_verify', True)
        
        if AI_USER_HEADER:
            headers[AI_USER_HEADER] = AI_USER

        verify_ssl = AI_SSL_VERIFY
        if verify_ssl and SSL_CERT_PATH:
            verify_ssl = SSL_CERT_PATH
            
        if "generativelanguage.googleapis.com" in API_ENDPOINT:
            API_URL = f"{API_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}"
            payload = {
                "contents": [{"parts": [{"text": full_prompt}]}],
                "generationConfig": {"temperature": AI_TEMPERATURE, "maxOutputTokens": AI_MAX_OUTPUT_TOKENS}
            }
        else:
            API_URL = f"{API_ENDPOINT}v1/chat/completions"
            headers['Authorization'] = f'Bearer {API_KEY}'
            payload = {
                "model": AI_MODEL,
                "messages": [{"role": "user", "content": full_prompt}],
                "temperature": AI_TEMPERATURE,
                "max_tokens": AI_MAX_OUTPUT_TOKENS,
                "user" : AI_USER
            }

        start_time = time.time()
        
        print(f"\n--- AI Request Details ---")
        print(f"  - Endpoint: {API_URL}")
        print(f"  - Model: {AI_MODEL}")
        print(f"  - Prompt Size: {prompt_chars:,} characters (~{estimated_tokens:,} tokens)")

        response = requests.post(API_URL, headers=headers, data=json.dumps(payload), verify=verify_ssl)
        response.raise_for_status()
        result = response.json()

        end_time = time.time()
        duration = end_time - start_time
        ai_metrics['processing_time_seconds'] = duration # Update the duration in the metrics dict
        print(f"  - AI Processing Time: {duration:.2f} seconds")
        
        # Add the processing time to the AsciiDoc details block
        ai_details_adoc.append(f"| AI Processing Time | {duration:.2f} seconds")
        ai_details_adoc.append("|===")

        if "generativelanguage.googleapis.com" in API_ENDPOINT:
            ai_recommendations = result['candidates'][0]['content']['parts'][0]['text']
        else:
            ai_recommendations = result['choices'][0]['message']['content']
        
        # Prepend the details block to the AI recommendations
        adoc_content.append("\n".join(ai_details_adoc))
        adoc_content.append(ai_recommendations)

    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nFailed to get AI recommendations: {e}\n====\n")
        return "\n".join(adoc_content), ai_metrics

    return "\n".join(adoc_content), ai_metrics
