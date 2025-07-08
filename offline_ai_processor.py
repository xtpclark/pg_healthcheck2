import json
import requests
import sys
import yaml
from pathlib import Path

def load_config(config_file_path):
    """Loads configuration settings from config.yaml."""
    try:
        with open(config_file_path, 'r') as f:
            settings = yaml.safe_load(f)
        
        # Ensure AI settings are loaded with defaults
        settings['ai_api_key'] = settings.get('ai_api_key', '')
        settings['ai_endpoint'] = settings.get('ai_endpoint', 'https://generativelanguage.googleapis.com/v1beta/models/')
        settings['ai_model'] = settings.get('ai_model', 'gemini-2.0-flash')
        settings['ai_user'] = settings.get('ai_user', 'anonymous')
        settings['ai_user_header'] = settings.get('ai_user_header', '')

        return settings
    except FileNotFoundError:
        print(f"Error: Config file {config_file_path} not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing config.yaml: {e}")
        sys.exit(1)

def run_offline_ai_analysis(config_file='config/config.yaml', structured_findings_file='adoc_out/nuance/structured_health_check_findings.json'):
    """
    Performs AI analysis offline by reading a structured findings JSON file
    and sending its prompt to the configured AI endpoint.
    """
    print(f"--- Starting Offline AI Analysis ---")

    # Load settings from config.yaml
    settings = load_config(config_file)

    # Construct the path to the structured findings file
    # Assuming structured_findings_file is relative to the script's execution directory
    # Adjust this path if your output directory structure is different
    try:
        structured_findings_path = Path(structured_findings_file)
        if not structured_findings_path.is_absolute():
            # Assuming it's relative to the current working directory
            # Or you might want it relative to the config file's parent directory
            structured_findings_path = Path.cwd() / structured_findings_path
        
        with open(structured_findings_path, 'r') as f:
            findings = json.load(f)
    except FileNotFoundError:
        print(f"Error: Structured findings file '{structured_findings_path}' not found. Please ensure the main health check script has been run to generate it.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding structured findings JSON: {e}. Please check the file format.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred while loading structured findings: {e}")
        sys.exit(1)

    # Extract the prompt that was prepared by the main script
    # This assumes 'run_recommendation' module's data contains 'prompt_sent'
    full_prompt = findings.get('run_recommendation', {}).get('data', {}).get('prompt_sent', '')

    if not full_prompt:
        print("Error: No AI prompt ('prompt_sent') found in the structured findings JSON under 'run_recommendation' module data.")
        print("Please ensure 'ai_analyze' is true in config.yaml and the main health check script was run successfully.")
        sys.exit(1)

    print("\n--- AI Prompt Prepared (truncated for display) ---")
    print(full_prompt[:500] + "..." if len(full_prompt) > 500 else full_prompt)
    print("--------------------------------------------------")

    # --- Make the AI API Call ---
    API_KEY = settings.get('ai_api_key', '')
    AI_ENDPOINT = settings.get('ai_endpoint', 'https://generativelanguage.googleapis.com/v1beta/models/')
    AI_MODEL = settings.get('ai_model', 'gemini-2.0-flash')

    if not API_KEY:
        print("Error: AI API key not found in config.yaml. Cannot perform offline AI analysis.")
        sys.exit(1)

    ai_recommendations = "Failed to get AI recommendations."
    try:
        # Prepare common headers
        headers = {'Content-Type': 'application/json'}
        # Add ai_user_header if configured
        ai_user_header_name = settings.get('ai_user_header', '')
        if ai_user_header_name and settings.get('ai_user'):
            headers[ai_user_header_name] = settings['ai_user']

        # --- Determine AI Provider and Construct Request ---
        if "generativelanguage.googleapis.com" in AI_ENDPOINT:
            # Google Gemini API
            API_URL = f"{AI_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}"
            payload = {
                "contents": [{"role": "user", "parts": [{"text": full_prompt}]}]
            }
        elif "api.openai.com" in AI_ENDPOINT:
            # OpenAI API (or compatible endpoint)
            API_URL = f"{AI_ENDPOINT}v1/chat/completions"
            payload = {
                "model": AI_MODEL,
                "messages": [{"role": "user", "content": full_prompt}]
            }
            # Add OpenAI specific Authorization header
            headers['Authorization'] = f'Bearer {API_KEY}'
        else:
            print(f"Error: Unsupported AI endpoint '{AI_ENDPOINT}'.")
            sys.exit(1)

        print(f"\n--- Sending prompt to AI model: {AI_MODEL} at {API_URL} ---")
        response = requests.post(API_URL, headers=headers, data=json.dumps(payload))
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        result = response.json()
        
        # --- Parse AI Response based on Provider ---
        if "generativelanguage.googleapis.com" in AI_ENDPOINT:
            if result and result.get('candidates') and result['candidates'][0].get('content') and result['candidates'][0]['content'].get('parts'):
                ai_recommendations = result['candidates'][0]['content']['parts'][0]['text']
            else:
                ai_recommendations = f"AI response structure unexpected (Gemini): {json.dumps(result)}"
                print(f"Warning: AI response structure unexpected (Gemini): {json.dumps(result)}")
        elif "api.openai.com" in AI_ENDPOINT:
            if result and result.get('choices') and result['choices'][0].get('message') and result['choices'][0]['message'].get('content'):
                ai_recommendations = result['choices'][0]['message']['content']
            else:
                ai_recommendations = f"AI response structure unexpected (OpenAI): {json.dumps(result)}"
                print(f"Warning: AI response structure unexpected (OpenAI): {json.dumps(result)}")

    except requests.exceptions.RequestException as e:
        print(f"Error communicating with AI API: {e}")
        ai_recommendations = f"Error communicating with AI API: {e}"
    except json.JSONDecodeError as e:
        print(f"Error decoding AI response JSON: {e}. Response text: {response.text}")
        ai_recommendations = f"Error decoding AI response JSON: {e}. Response text: {response.text}"
    except Exception as e:
        print(f"An unexpected error occurred during AI processing: {e}")
        ai_recommendations = f"An unexpected error occurred during AI processing: {e}"

    print("\n--- AI Recommendations ---")
    print(ai_recommendations)
    print("--------------------------")

if __name__ == '__main__':
    # You can pass config file and structured findings file as arguments
    # Example: python3 offline_ai_processor.py --config config/my_special_config.yaml --findings adoc_out/my_company/structured_findings.json
    import argparse
    parser = argparse.ArgumentParser(description="Run offline AI analysis on PostgreSQL health check findings.")
    parser.add_argument('--config', type=str, default='config/config.yaml',
                        help='Path to the config.yaml file.')
    parser.add_argument('--findings', type=str, default='adoc_out/nuance/structured_health_check_findings.json',
                        help='Path to the structured_health_check_findings.json file.')
    args = parser.parse_args()

    run_offline_ai_analysis(args.config, args.findings)

