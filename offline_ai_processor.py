import json
import requests
import sys
import yaml
from pathlib import Path
import time # Import time module for measuring analysis duration

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
        settings['ssl_cert_path'] = settings.get('ssl_cert_path', '')
        settings['ai_temperature'] = settings.get('ai_temperature', 0.7) # Load AI temperature
        settings['ai_max_output_tokens'] = settings.get('ai_max_output_tokens', 2048) # Load AI max output tokens

        return settings
    except FileNotFoundError:
        print(f"Error: Config file {config_file_path} not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing config.yaml: {e}")
        sys.exit(1)

def run_offline_ai_analysis(config_file='config/config.yaml', structured_findings_file='structured_health_check_findings.json'):
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
    # Check both enhanced and original modules since enhanced is primary
    full_prompt = findings.get('run_recommendation_enhanced', {}).get('data', {}).get('prompt_sent', '')
    
    # If not found in enhanced module, try the original module (fallback)
    if not full_prompt:
        full_prompt = findings.get('run_recommendation', {}).get('data', {}).get('prompt_sent', '')

    if not full_prompt:
        print("Error: No AI prompt ('prompt_sent') found in the structured findings JSON.")
        print("Checked both 'run_recommendation_enhanced' and 'run_recommendation' modules.")
        print("Please ensure 'ai_analyze' is true in config.yaml and the main health check script was run successfully.")
        sys.exit(1)

    print("\n--- AI Prompt Prepared (truncated for display) ---")
    print(full_prompt[:500] + "..." if len(full_prompt) > 500 else full_prompt)
    print("--------------------------------------------------")

    # --- Make the AI API Call ---
    API_KEY = settings.get('ai_api_key', '')
    AI_ENDPOINT = settings.get('ai_endpoint', 'https://generativelanguage.googleapis.com/v1beta/models/')
    AI_MODEL = settings.get('ai_model', 'gemini-2.0-flash')
    AI_USER = settings.get('ai_user', 'anonymous')
    AI_USER_HEADER_NAME = settings.get('ai_user_header', '')
    SSL_CERT_PATH = settings.get('ssl_cert_path', '')
    AI_TEMPERATURE = settings.get('ai_temperature', 0.7) # Get AI temperature
    AI_MAX_OUTPUT_TOKENS = settings.get('ai_max_output_tokens', 2048) # Get AI max output tokens

    if not API_KEY:
        print("Error: AI API key not found in config.yaml. Cannot perform offline AI analysis.")
        sys.exit(1)

    # Set SSL verification
    verify_ssl = SSL_CERT_PATH if SSL_CERT_PATH else True

    ai_recommendations = "Failed to get AI recommendations."
    try:
        # Prepare common headers
        headers = {'Content-Type': 'application/json'}
        # Add custom AI user header if configured
        if AI_USER_HEADER_NAME and AI_USER:
            headers[AI_USER_HEADER_NAME] = AI_USER

        # --- Determine AI Provider and Construct Request ---
        if "generativelanguage.googleapis.com" in AI_ENDPOINT:
            # Google Gemini API
            API_URL = f"{AI_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}"
            payload = {
                "contents": [{"role": "user", "parts": [{"text": full_prompt}]}],
                "generationConfig": { # Add generation config for Gemini
                    "temperature": AI_TEMPERATURE,
                    "maxOutputTokens": AI_MAX_OUTPUT_TOKENS
                }
            }
        else:
            # Assume OpenAI API (or compatible endpoint)
            API_URL = f"{AI_ENDPOINT}v1/chat/completions" # Assuming standard OpenAI chat completions path
            payload = {
                "model": AI_MODEL,
                "messages": [{"role": "user", "content": full_prompt}],
                "user": AI_USER,
                "temperature": AI_TEMPERATURE, # Add temperature for OpenAI-compatible
                "max_tokens": AI_MAX_OUTPUT_TOKENS # Add max_tokens for OpenAI-compatible
            }
            # Add OpenAI specific Authorization header
            headers['Authorization'] = f'Bearer {API_KEY}'

        print(f"\n--- Sending prompt to AI model: {AI_MODEL} at {API_URL} ---")
        
        # Record start time for analysis
        start_time = time.time()

        response = requests.post(API_URL, headers=headers, data=json.dumps(payload), verify=verify_ssl)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        # Record end time for analysis
        end_time = time.time()
        time_taken = end_time - start_time # Calculate time taken

        result = response.json()
        
        # Print AI analysis statistics
        print(f"AI Endpoint: {AI_ENDPOINT}")
        print(f"AI Model: {AI_MODEL}")
        print(f"AI Temperature: {AI_TEMPERATURE}")
        print(f"AI Max Output Tokens: {AI_MAX_OUTPUT_TOKENS}")
        print(f"Prompt Characters: {len(full_prompt)}")
        print(f"Analysis Time: {round(time_taken, 2)} seconds")

        # --- Parse AI Response based on Provider ---
        if "generativelanguage.googleapis.com" in AI_ENDPOINT:
            if result and result.get('candidates') and result['candidates'][0].get('content') and result['candidates'][0]['content'].get('parts'):
                ai_recommendations = result['candidates'][0]['content']['parts'][0]['text']
                print(f"Response Characters: {len(ai_recommendations)}")
            else:
                ai_recommendations = f"AI response structure unexpected (Gemini): {json.dumps(result)}"
                print(f"Warning: AI response structure unexpected (Gemini): {json.dumps(result)}")
        else: # Assume OpenAI-compatible for any other endpoint
            if result and result.get('choices') and result['choices'][0].get('message') and result['choices'][0]['message'].get('content'):
                ai_recommendations = result['choices'][0]['message']['content']
                print(f"Response Characters: {len(ai_recommendations)}")
                if 'usage' in result:
                    print(f"Prompt Tokens: {result['usage'].get('prompt_tokens')}")
                    print(f"Completion Tokens: {result['usage'].get('completion_tokens')}")
                    print(f"Total Tokens: {result['usage'].get('total_tokens')}")
            else:
                ai_recommendations = f"AI response structure unexpected (OpenAI-compatible): {json.dumps(result)}"
                print(f"Warning: AI response structure unexpected (OpenAI-compatible): {json.dumps(result)}")

    except requests.exceptions.RequestException as e:
        print(f"Error communicating with AI API: {e}")
        ai_recommendations = f"Error communicating with AI API: {e}"
    except json.JSONDecodeError as e:
        print(f"Error decoding AI response JSON: {e}. Response text: {response.text}")
        ai_recommendations = f"Error decoding AI response JSON: {e}. Response text: {response.text}"
    except Exception as e:
        print(f"An unexpected error occurred during AI processing: {e}")
        ai_recommendations = f"An unexpected error occurred during AI processing: {e}"

    print("\n--- AI Recommendations ---\n")
    print(ai_recommendations)
    print("--------------------------")

if __name__ == '__main__':
    # You can pass config file and structured findings file as arguments
    # Example: python3 offline_ai_processor.py --config config/my_special_config.yaml --findings adoc_out/my_company/structured_findings.json
    import argparse
    parser = argparse.ArgumentParser(description="Run offline AI analysis on PostgreSQL health check findings.")
    parser.add_argument('--config', type=str, default='config/config.yaml',
                        help='Path to the config.yaml file.')
    parser.add_argument('--findings', type=str, default='structured_health_check_findings.json',
                        help='Path to the structured_health_check_findings.json file.')
    args = parser.parse_args()

    run_offline_ai_analysis(args.config, args.findings)
