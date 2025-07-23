import json
import requests
import sys
import yaml
from pathlib import Path
import time
import argparse
# --- Import the prompt generator ---
# This path might need to be adjusted based on your final directory structure.
from modules.dynamic_prompt_generator import generate_dynamic_prompt

def load_config(config_file_path):
    """Loads configuration settings from config.yaml."""
    try:
        with open(config_file_path, 'r') as f:
            return yaml.safe_load(f)
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(f"Error loading config file {config_file_path}: {e}")
        sys.exit(1)

def run_offline_ai_analysis(config_file, findings_file, template_file):
    """
    Performs AI analysis by loading raw structured data, generating a prompt
    using a specified template, and sending it to the AI endpoint.
    """
    print(f"--- Starting Offline AI Analysis ---")
    settings = load_config(config_file)
    
    # --- Override the template in settings with the command-line argument ---
    if template_file:
        settings['prompt_template'] = Path(template_file).name
    
    try:
        with open(findings_file, 'r') as f:
            all_structured_findings = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading findings file {findings_file}: {e}")
        sys.exit(1)

    # --- Generate the prompt on-the-fly ---
    try:
        print(f"--- Generating prompt using template: {settings.get('prompt_template', 'prompt_template.j2')} ---")
        dynamic_analysis = generate_dynamic_prompt(all_structured_findings, settings)
        full_prompt = dynamic_analysis['prompt']
    except Exception as e:
        print(f"Error generating dynamic prompt: {e}")
        sys.exit(1)

    print("\n--- AI Prompt Prepared (truncated for display) ---")
    print(full_prompt[:500] + "...")
    print("--------------------------------------------------")

    API_KEY = settings.get('ai_api_key')
    if not API_KEY:
        print("Error: AI API key not found in config.yaml.")
        sys.exit(1)

    try:
        # --- MODIFIED: Load new settings for enterprise proxies ---
        AI_ENDPOINT = settings.get('ai_endpoint')
        AI_MODEL = settings.get('ai_model')
        AI_TEMPERATURE = settings.get('ai_temperature', 0.7)
        AI_MAX_OUTPUT_TOKENS = settings.get('ai_max_output_tokens', 2048)
        
        # Load settings for user identification and SSL
        AI_USER = settings.get('ai_user', 'anonymous')
        AI_USER_HEADER = settings.get('ai_user_header', '') # e.g., 'X-User-ID'
        SSL_CERT_PATH = settings.get('ssl_cert_path', '')
        AI_SSL_VERIFY = settings.get('ai_ssl_verify', True)

        headers = {'Content-Type': 'application/json'}
        
        # --- MODIFIED: Add user header if specified ---
        if AI_USER_HEADER:
            headers[AI_USER_HEADER] = AI_USER

        # --- MODIFIED: Determine SSL verification setting ---
        verify_ssl = AI_SSL_VERIFY
        if verify_ssl and SSL_CERT_PATH:
            verify_ssl = SSL_CERT_PATH # Path to custom cert bundle
        
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

        # --- MODIFIED: Added the 'verify' parameter to the request ---
        print(f"--- Sending request to AI Endpoint. SSL Verification: {verify_ssl} ---")
        response = requests.post(API_URL, headers=headers, data=json.dumps(payload), verify=verify_ssl)
        response.raise_for_status()
        result = response.json()

        if "generativelanguage.googleapis.com" in AI_ENDPOINT:
            ai_recommendations = result['candidates'][0]['content']['parts'][0]['text']
        else:
            ai_recommendations = result['choices'][0]['message']['content']
        
        print("\n--- AI Recommendations ---\n")
        print(ai_recommendations)

    except Exception as e:
        print(f"Error communicating with AI API: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run offline AI analysis on PostgreSQL health check findings.")
    parser.add_argument('--config', default='config/config.yaml', help='Path to the config.yaml file.')
    parser.add_argument('--findings', required=True, help='Path to the structured_health_check_findings.json file.')
    parser.add_argument('--template', help='Name of the Jinja2 template file to use (e.g., executive_summary_template.j2). Overrides config.yaml.')
    args = parser.parse_args()

    run_offline_ai_analysis(args.config, args.findings, args.template)
