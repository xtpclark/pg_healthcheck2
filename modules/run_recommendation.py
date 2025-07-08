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
        return [convert_to_json_serializable(elem) for elem in obj]
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

    # --- Step 2: Make the AI API Call ---
    # Retrieve generic AI settings from the 'settings' dictionary
    API_KEY = settings.get('ai_api_key', '')
    AI_USER = settings.get('ai_user', 'anonymous')
    AI_ENDPOINT = settings.get('ai_endpoint', 'https://generativelanguage.googleapis.com/v1beta/models/') # Default for Gemini
    AI_MODEL = settings.get('ai_model', 'gemini-2.0-flash') # Default model

    API_URL = f"{AI_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}"

    ai_recommendations = "Failed to get AI recommendations."
    if not API_KEY:
        ai_recommendations = "AI analysis skipped: AI API key not found in config.yaml."
        print("Warning: AI API key not found in config.yaml. AI analysis skipped.")
        structured_data["ai_analysis"]["status"] = "skipped"
        structured_data["ai_analysis"]["reason"] = "API key missing"
    else:
        try:
            payload = {
                "contents": [{"role": "user", "parts": [{"text": full_prompt}]}]
            }
            headers = {'Content-Type': 'application/json'}

            # Make the POST request to the AI API
            response = requests.post(API_URL, headers=headers, data=json.dumps(payload))
            response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

            result = response.json()
            
            if result and result.get('candidates') and result['candidates'][0].get('content') and result['candidates'][0]['content'].get('parts'):
                ai_recommendations = result['candidates'][0]['content']['parts'][0]['text']
                structured_data["ai_analysis"]["status"] = "success"
            else:
                ai_recommendations = f"AI response structure unexpected: {json.dumps(result)}"
                structured_data["ai_analysis"]["status"] = "error"
                structured_data["ai_analysis"]["details"] = f"Unexpected response: {json.dumps(result)}"
                print(f"Warning: AI response structure unexpected: {json.dumps(result)}")

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

    structured_data["ai_analysis"]["recommendations"] = ai_recommendations # Store the AI's raw response

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
