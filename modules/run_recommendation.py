import json
from decimal import Decimal
from datetime import datetime
import requests # Import requests for making HTTP calls

# Helper function to convert Decimal and datetime objects to JSON-serializable types recursively
def convert_to_json_serializable(obj):
    if isinstance(obj, Decimal):
        return float(obj) # Convert Decimal to float
    elif isinstance(obj, datetime):
        return obj.isoformat() # Convert datetime to ISO 8601 string
    elif isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj] # Corrected: 'item' for elements in the list 'obj'
    else:
        return obj

def run_recommendation(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Aggregates findings from various health check modules, constructs a prompt,
    sends it to an AI for analysis, and integrates the AI's recommendations
    into the report.
    """
    adoc_content = ["=== Recommendations", "Provides aggregated recommendations based on the health check findings."]
    structured_data = {"ai_analysis": {}, "prompt_sent": ""} # To store AI related data

    if settings['show_qry'] == 'true':
        adoc_content.append("Recommendation generation logic involves AI analysis based on collected data.")
        adoc_content.append("----")

    # --- Step 1: Construct the AI Prompt ---
    prompt_parts = []
    prompt_parts.append("Analyze the following PostgreSQL health check report data and provide actionable, prioritized recommendations.\n\n")
    prompt_parts.append(f"Analysis initiated by user: {settings.get('ai_user', 'anonymous')}\n\n") # Include AI user
    prompt_parts.append("--- PostgreSQL Health Check Findings ---\n\n")

    # Prepare findings for prompt by converting non-JSON-serializable types
    # Create a copy to avoid modifying the original all_structured_findings in place
    findings_for_prompt = convert_to_json_serializable(all_structured_findings)

    # Iterate through all collected structured findings
    for module_name, module_findings in findings_for_prompt.items():
        prompt_parts.append(f"** Module: {module_name.replace('_', ' ').title()} **\n")
        
        if module_findings.get("status") == "failed_to_load":
            prompt_parts.append(f"  Status: Failed to load/execute. Error: {module_findings.get('error', 'Unknown error')}\n")
        elif module_findings.get("note"):
            prompt_parts.append(f"  Note: {module_findings.get('note')}\n")
        elif module_findings.get("status") == "not_applicable":
            prompt_parts.append(f"  Status: Not Applicable. Reason: {module_findings.get('reason', 'N/A')}\n")
        elif module_findings.get("status") == "error":
            prompt_parts.append(f"  Status: Query Error. Details: {json.dumps(module_findings.get('details', {}), indent=2)}\n")
        elif module_findings.get("status") == "success" and module_findings.get("data") is not None:
            # For successful data, dump the raw JSON data
            prompt_parts.append(f"  Data:\n{json.dumps(module_findings['data'], indent=2)}\n")
        else:
            prompt_parts.append("  Status: No specific data or unhandled status.\n")
        prompt_parts.append("\n") # Add a newline for separation between modules

    prompt_parts.append("\nBased on these findings, provide a concise, prioritized list of recommendations. For each recommendation, briefly explain its importance and suggest specific actions. Focus on performance, stability, and security improvements relevant to a PostgreSQL database, especially considering it might be an AWS RDS Aurora instance if 'is_aurora' is true in settings.\n")

    full_prompt = "".join(prompt_parts)
    structured_data["prompt_sent"] = full_prompt # Store the prompt that was sent

    # --- Step 2: Make the AI API Call (Conditional based on ai_run_integrated) ---
    ai_recommendations = "AI analysis was not performed."
    
    if settings.get('ai_analyze', False): # Check master switch from config.yaml
        if settings.get('ai_run_integrated', True): # Check if integrated run is enabled
            # Retrieve generic AI settings from the 'settings' dictionary
            API_KEY = settings.get('ai_api_key', '')
            AI_ENDPOINT = settings.get('ai_endpoint', 'https://generativelanguage.googleapis.com/v1beta/models/')
            AI_MODEL = settings.get('ai_model', 'gemini-2.0-flash')
            AI_USER = settings.get('ai_user', 'anonymous') # Get AI user
            AI_USER_HEADER_NAME = settings.get('ai_user_header', '') # Get custom user header name
            SSL_CERT_PATH = settings.get('ssl_cert_path', '') # NEW: Get SSL cert path

            # Prepare common headers
            headers = {'Content-Type': 'application/json'}
            # Add custom ai_user_header if configured
            if AI_USER_HEADER_NAME and AI_USER:
                headers[AI_USER_HEADER_NAME] = AI_USER

            # Set SSL verification
            verify_ssl = SSL_CERT_PATH if SSL_CERT_PATH else True # NEW: Add this

            if not API_KEY:
                ai_recommendations = "AI analysis skipped: AI API key not found in config.yaml."
                print("Warning: AI API key not found in config.yaml. AI analysis skipped.")
                structured_data["ai_analysis"]["status"] = "skipped"
                structured_data["ai_analysis"]["reason"] = "API key missing"
            else:
                try:
                    # --- Determine AI Provider and Construct Request ---
                    if "generativelanguage.googleapis.com" in AI_ENDPOINT:
                        # Google Gemini API
                        API_URL = f"{AI_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}"
                        payload = {
                            "contents": [{"role": "user", "parts": [{"text": full_prompt}]}]
                        }
                        # Headers already prepared
                    else: # Assume OpenAI-compatible for any other endpoint
                        # OpenAI API (or compatible endpoint, including corporate proxy)
                        API_URL = f"{AI_ENDPOINT}v1/chat/completions" # Standard OpenAI chat completions path
                        payload = {
                            "model": AI_MODEL,
                            "messages": [{"role": "user", "content": full_prompt}],
                            "user": AI_USER # Add 'user' field to payload body
                        }
                        # Add Authorization header for OpenAI/compatible
                        headers['Authorization'] = f'Bearer {API_KEY}'

                    # Make the POST request to the AI API
                    response = requests.post(API_URL, headers=headers, data=json.dumps(payload), verify=verify_ssl) # NEW: Add verify
                    response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

                    result = response.json()
                    
                    # --- Parse AI Response based on Provider ---
                    if "generativelanguage.googleapis.com" in AI_ENDPOINT:
                        # Parse Gemini response
                        if result and result.get('candidates') and result['candidates'][0].get('content') and result['candidates'][0]['content'].get('parts'):
                            ai_recommendations = result['candidates'][0]['content']['parts'][0]['text']
                            structured_data["ai_analysis"]["status"] = "success"
                        else:
                            ai_recommendations = f"AI response structure unexpected (Gemini): {json.dumps(result)}"
                            structured_data["ai_analysis"]["status"] = "error"
                            structured_data["ai_analysis"]["details"] = f"Unexpected response: {json.dumps(result)}"
                            print(f"Warning: AI response structure unexpected (Gemini): {json.dumps(result)}")
                    else: # Assume OpenAI-compatible for any other endpoint
                        # Parse OpenAI/compatible response
                        if result and result.get('choices') and result['choices'][0].get('message') and result['choices'][0]['message'].get('content'):
                            ai_recommendations = result['choices'][0]['message']['content']
                            structured_data["ai_analysis"]["status"] = "success"
                        else:
                            ai_recommendations = f"AI response structure unexpected (OpenAI-compatible): {json.dumps(result)}"
                            structured_data["ai_analysis"]["status"] = "error"
                            structured_data["ai_analysis"]["details"] = f"Unexpected response: {json.dumps(result)}"
                            print(f"Warning: AI response structure unexpected (OpenAI-compatible): {json.dumps(result)}")

                except requests.exceptions.RequestException as e:
                    ai_recommendations = f"Error communicating with AI API: {e}"
                    structured_data["ai_analysis"]["status"] = "error"
                    structured_data["ai_analysis"]["details"] = str(e)
                    print(f"Error communicating with AI API: {e}")
                except json.JSONDecodeError as e:
                    ai_recommendations = f"Error decoding AI response JSON: {e}. Response text: {response.text}"
                    structured_data["ai_analysis"]["status"] = "error"
                    structured_data["ai_analysis"]["details"] = f"JSON decode error: {e}, Response: {response.text}"
                    print(f"Error decoding AI response JSON: {e}. Response text: {response.text}")
                except Exception as e:
                    ai_recommendations = f"An unexpected error occurred during AI processing: {e}"
                    structured_data["ai_analysis"]["status"] = "error"
                    structured_data["ai_analysis"]["details"] = str(e)
                    print(f"An unexpected error occurred during AI processing: {e}")
        else:
            # AI analysis is enabled but configured for offline/separate run
            ai_recommendations = "[NOTE]\n====\nAI analysis is enabled but configured for offline processing. The AI prompt has been generated and saved to 'structured_health_check_findings.json'.\n\nTo get AI recommendations from this data:\n\n1.  **Ensure you have network access** to your chosen AI provider's API (e.g., via VPN if necessary).\n2.  **Use a separate script or tool** to read `structured_health_check_findings.json`.\n3.  **Extract the `prompt_sent` field** from the JSON. This contains the full prompt prepared for the AI.\n4.  **Send this `prompt_sent` string to your AI API endpoint** (e.g., Google Gemini or an OpenAI-compatible endpoint).\n5.  **Integrate the AI's response** into your report or review it separately.\n\nExample (Python conceptual for offline script):\n```python\nimport json\nimport requests\n\n# --- Configuration for your offline script ---\nAPI_KEY = \"YOUR_AI_API_KEY\"\nAI_ENDPOINT = \"[https://generativelanguage.googleapis.com/v1beta/models/](https://generativelanguage.googleapis.com/v1beta/models/)\" # Or your OpenAI-compatible endpoint\nAI_MODEL = \"gemini-2.0-flash\" # Or \"gpt-3.5-turbo\"\nAI_USER = \"offline_user\" # Default user for offline processing\nAI_USER_HEADER_NAME = \"X-User-ID\" # Custom user header name for proxy\nSSL_CERT_PATH = \"\" # Or '/path/to/your/cert.pem' for SSL verification\n\n# --- Load the saved structured findings ---\nwith open('adoc_out/nuance/structured_health_check_findings.json', 'r') as f:\n    findings = json.load(f)\n\n# Extract the prompt that was prepared by the main script\nfull_prompt = findings.get('run_recommendation', {}).get('data', {}).get('prompt_sent', '')\n\nif full_prompt:\n    print(\"\\n--- Sending to AI for offline analysis ---\")\n    # Prepare headers for offline script\n    offline_headers = {'Content-Type': 'application/json'}\n    if AI_USER_HEADER_NAME and AI_USER:\n        offline_headers[AI_USER_HEADER_NAME] = AI_USER\n\n    # Set SSL verification for offline script\n    offline_verify_ssl = SSL_CERT_PATH if SSL_CERT_PATH else True\n\n    # Determine AI Provider and Construct Request for offline script\n    if \"generativelanguage.googleapis.com\" in AI_ENDPOINT:\n        API_URL_OFFLINE = f\"{AI_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}\"\n        payload_offline = {\"contents\": [{\"role\": \"user\", \"parts\": [{\"text\": full_prompt}]}]}\n    else: # Assume OpenAI-compatible for any other endpoint\n        API_URL_OFFLINE = f\"{AI_ENDPOINT}v1/chat/completions\"\n        payload_offline = {\n            \"model\": AI_MODEL,\n            \"messages\": [{\"role\": \"user\", \"content\": full_prompt}],\n            \"user\": AI_USER # Add 'user' field to payload body for offline\n        }\n        offline_headers['Authorization'] = f'Bearer {API_KEY}'\n\n    try:\n        response = requests.post(API_URL_OFFLINE, headers=offline_headers, data=json.dumps(payload_offline), verify=offline_verify_ssl)\n        response.raise_for_status()\n        result = response.json()\n        \n        if \"generativelanguage.googleapis.com\" in AI_ENDPOINT:\n            ai_response = result['candidates'][0]['content']['parts'][0]['text']\n        else: # Assume OpenAI-compatible for any other endpoint\n            ai_response = result['choices'][0]['message']['content']\n\n        print(\"\\n--- AI Recommendations (Offline) ---\")\n        print(ai_response)\n    except requests.exceptions.RequestException as e:\n        print(f\"Error during offline AI call: {e}\")\n    except Exception as e:\n        print(f\"An unexpected error occurred: {e}\")\nelse:\n    print(\"No AI prompt found in the structured findings for offline analysis.\")\n```\n====\n"
            structured_data["ai_analysis"]["status"] = "offline_mode"
            structured_data["ai_analysis"]["note"] = "AI analysis skipped for integrated run; prompt saved for offline processing."
    else:
        # AI analysis is completely disabled (ai_analyze is false)
        ai_recommendations = "[NOTE]\n====\nAI analysis is disabled in config.yaml (ai_analyze is false). No AI recommendations will be generated.\n====\n"
        structured_data["ai_analysis"]["status"] = "disabled"
        structured_data["ai_analysis"]["note"] = "AI analysis is disabled by configuration."

    structured_data["ai_analysis"]["recommendations_output"] = ai_recommendations # Store the AI's raw response or status

    # --- Step 3: Integrate AI Response into AsciiDoc Content ---
    adoc_content.append("\n=== AI-Generated Recommendations\n")
    adoc_content.append(ai_recommendations)
    
    adoc_content.append("[TIP]\n====\n"
                       "Review all sections of this report for specific findings and recommendations. "
                       "Prioritize issues that directly impact your application's performance, stability, or security, "
                       "such as high CPU usage, long-running queries, or unindexed foreign keys. "
                       "Always test recommendations in a non-production environment before applying them to your main database.\n"
                       "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                           "For AWS RDS Aurora, many recommendations involve adjusting parameters in the DB cluster parameter group, "
                           "optimizing queries, or scaling instance types. "
                           "Leverage AWS CloudWatch and Performance Insights for deeper analysis of metrics and query performance. "
                           "Consider using AWS Database Migration Service (DMS) for major version upgrades or schema changes.\n"
                           "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
