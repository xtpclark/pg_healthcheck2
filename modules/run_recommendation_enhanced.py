#!/usr/bin/env python3
"""
Enhanced AI Recommendation Module with Dynamic Prompt Generation

This module provides intelligent, context-aware AI recommendations based on
dynamic analysis of collected PostgreSQL metrics and their severity levels.
"""

import json
import time
import requests
from decimal import Decimal
from datetime import datetime, timedelta

# Import the dynamic prompt generator
try:
    from .dynamic_prompt_generator import generate_dynamic_prompt, convert_to_json_serializable
except ImportError:
    # Fallback if dynamic_prompt_generator is not available
    def generate_dynamic_prompt(all_structured_findings, settings):
        return {"prompt": "Dynamic prompt generation not available."}
    
    def convert_to_json_serializable(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return obj.total_seconds()
        elif isinstance(obj, dict):
            return {k: convert_to_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_json_serializable(item) for item in obj]
        else:
            return obj

def run_recommendation_enhanced(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Enhanced AI recommendation system with dynamic prompt generation.
    
    This function analyzes collected metrics, determines severity levels,
    and generates context-aware AI recommendations based on the findings.
    """
    adoc_content = ["Provides intelligent, context-aware recommendations based on dynamic analysis of database metrics.\n"]
    structured_data = {"ai_analysis": {}, "dynamic_analysis": {}, "prompt_sent": ""}

    if settings['show_qry'] == 'true':
        adoc_content.append("Enhanced recommendation generation uses dynamic prompt analysis based on metric severity.")
        adoc_content.append("----")

    # --- Step 1: Generate Dynamic Prompt ---
    try:
        dynamic_analysis = generate_dynamic_prompt(all_structured_findings, settings)
        full_prompt = dynamic_analysis['prompt']
        structured_data["dynamic_analysis"] = dynamic_analysis
        structured_data["prompt_sent"] = full_prompt
        
        # Add dynamic analysis summary to report
        adoc_content.append("** Dynamic Analysis Summary **\n")
        adoc_content.append(f"- Total Issues Detected: {dynamic_analysis.get('total_issues', 0)}\n")
        adoc_content.append(f"- Critical Issues: {len(dynamic_analysis.get('critical_issues', []))}\n")
        adoc_content.append(f"- High Priority Issues: {len(dynamic_analysis.get('high_priority_issues', []))}\n")
        adoc_content.append(f"- Medium Priority Issues: {len(dynamic_analysis.get('medium_priority_issues', []))}\n\n")
        
        if dynamic_analysis.get('critical_issues'):
            adoc_content.append("🚨 **CRITICAL ISSUES REQUIRING IMMEDIATE ATTENTION:**\n")
            for issue in dynamic_analysis['critical_issues']:
                adoc_content.append(f"- {issue['metric'].replace('_', ' ').title()}: {issue['analysis']['reasoning']}\n")
            adoc_content.append("\n")
        
        if dynamic_analysis.get('high_priority_issues'):
            adoc_content.append("⚠️ **HIGH PRIORITY ISSUES:**\n")
            for issue in dynamic_analysis['high_priority_issues']:
                adoc_content.append(f"- {issue['metric'].replace('_', ' ').title()}: {issue['analysis']['reasoning']}\n")
            adoc_content.append("\n")
            
    except Exception as e:
        # Fallback to standard prompt generation
        adoc_content.append(f"[WARNING]\n====\nDynamic prompt generation failed: {e}\nFalling back to standard prompt generation.\n====\n")
        
        # Generate standard prompt as fallback
        prompt_parts = []
        prompt_parts.append("Analyze the following PostgreSQL health check report data and provide actionable, prioritized recommendations.\n\n")
        prompt_parts.append(f"Analysis initiated by user: {settings.get('ai_user', 'anonymous')}\n\n")
        prompt_parts.append("--- PostgreSQL Health Check Findings ---\n\n")

        findings_for_prompt = convert_to_json_serializable(all_structured_findings)
        
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
                prompt_parts.append(f"  Data:\n{json.dumps(module_findings['data'], indent=2)}\n")
            else:
                prompt_parts.append("  Status: No specific data or unhandled status.\n")
            prompt_parts.append("\n")

        prompt_parts.append("\nBased on these findings, provide a concise, prioritized list of recommendations. For each recommendation, briefly explain its importance and suggest specific actions. Focus on performance, stability, and security improvements relevant to a PostgreSQL database, especially considering it might be an AWS RDS Aurora instance if 'is_aurora' is true in settings.\n")

        # Add specific guidance based on detected issues
        critical_issues = dynamic_analysis.get('critical_issues', [])
        high_priority_issues = dynamic_analysis.get('high_priority_issues', [])

        if critical_issues or high_priority_issues:
            prompt_parts.append("FOCUS AREAS FOR RECOMMENDATIONS:\n")
            if any('connection' in issue['metric'] for issue in critical_issues + high_priority_issues):
                prompt_parts.append("- Connection management and pooling optimization\n")
            if any('query' in issue['metric'] for issue in critical_issues + high_priority_issues):
                prompt_parts.append("- Query performance optimization and indexing\n")
            if any('vacuum' in issue['metric'] or 'bloat' in issue['metric'] for issue in critical_issues + high_priority_issues):
                prompt_parts.append("- Vacuum and maintenance scheduling\n")
            if any('security' in issue['metric'] or 'ssl' in issue['metric'] for issue in critical_issues + high_priority_issues):
                prompt_parts.append("- Security hardening and access controls\n")
            if any('aurora' in issue['metric'] for issue in critical_issues + high_priority_issues):
                prompt_parts.append("- AWS Aurora-specific optimizations\n")
            if any('index' in issue['metric'] for issue in critical_issues + high_priority_issues):
                prompt_parts.append("- Index optimization with read replica considerations\n")
            prompt_parts.append("\n")
        
        # Add read replica considerations for index-related issues
        if any('index' in issue['metric'] for issue in critical_issues + high_priority_issues):
            prompt_parts.append("⚠️ CRITICAL INDEX CONSIDERATIONS ⚠️\n")
            prompt_parts.append("When analyzing index usage and making recommendations:\n")
            prompt_parts.append("- Index usage statistics are tracked per-instance\n")
            prompt_parts.append("- 'Unused' indexes on primary may be heavily used on read replicas\n")
            prompt_parts.append("- Always check index usage on ALL nodes before removal\n")
            prompt_parts.append("- Consider query routing patterns in your application\n")
            prompt_parts.append("- Test index changes thoroughly in staging environment\n\n")

        full_prompt = "".join(prompt_parts)
        structured_data["prompt_sent"] = full_prompt
        structured_data["dynamic_analysis"] = {"status": "fallback", "error": str(e)}

    # --- Step 2: Make the AI API Call ---
    ai_recommendations = "AI analysis was not performed."
    
    if settings.get('ai_analyze', False):
        if settings.get('ai_run_integrated', True):
            # Retrieve AI settings
            API_KEY = settings.get('ai_api_key', '')
            AI_ENDPOINT = settings.get('ai_endpoint', 'https://generativelanguage.googleapis.com/v1beta/models/')
            AI_MODEL = settings.get('ai_model', 'gemini-2.0-flash')
            AI_USER = settings.get('ai_user', 'anonymous')
            AI_USER_HEADER_NAME = settings.get('ai_user_header', '')
            SSL_CERT_PATH = settings.get('ssl_cert_path', '')
            AI_TEMPERATURE = settings.get('ai_temperature', 0.7)
            AI_MAX_OUTPUT_TOKENS = settings.get('ai_max_output_tokens', 2048)

            # Prepare headers
            headers = {'Content-Type': 'application/json'}
            if AI_USER_HEADER_NAME and AI_USER:
                headers[AI_USER_HEADER_NAME] = AI_USER

            verify_ssl = SSL_CERT_PATH if SSL_CERT_PATH else True

            if not API_KEY:
                ai_recommendations = "AI analysis skipped: AI API key not found in config.yaml."
                print("Warning: AI API key not found in config.yaml. AI analysis skipped.")
                structured_data["ai_analysis"]["status"] = "skipped"
                structured_data["ai_analysis"]["reason"] = "API key missing"
            else:
                try:
                    # Determine AI Provider and Construct Request
                    if "generativelanguage.googleapis.com" in AI_ENDPOINT:
                        # Google Gemini API
                        API_URL = f"{AI_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}"
                        payload = {
                            "contents": [{"role": "user", "parts": [{"text": full_prompt}]}],
                            "generationConfig": {
                                "temperature": AI_TEMPERATURE,
                                "maxOutputTokens": AI_MAX_OUTPUT_TOKENS
                            }
                        }
                    else:
                        # OpenAI-compatible API
                        API_URL = f"{AI_ENDPOINT}v1/chat/completions"
                        payload = {
                            "model": AI_MODEL,
                            "messages": [{"role": "user", "content": full_prompt}],
                            "user": AI_USER,
                            "temperature": AI_TEMPERATURE,
                            "max_tokens": AI_MAX_OUTPUT_TOKENS
                        }
                        headers['Authorization'] = f'Bearer {API_KEY}'

                    # Record timing
                    start_time = time.time()
                    response = requests.post(API_URL, headers=headers, data=json.dumps(payload), verify=verify_ssl)
                    response.raise_for_status()
                    end_time = time.time()
                    time_taken = end_time - start_time

                    result = response.json()
                    
                    # Store analysis details
                    structured_data["ai_analysis"]["endpoint"] = AI_ENDPOINT
                    structured_data["ai_analysis"]["model"] = AI_MODEL
                    structured_data["ai_analysis"]["temperature"] = AI_TEMPERATURE
                    structured_data["ai_analysis"]["max_output_tokens_requested"] = AI_MAX_OUTPUT_TOKENS
                    structured_data["ai_analysis"]["prompt_characters"] = len(full_prompt)
                    structured_data["ai_analysis"]["analysis_time_seconds"] = round(time_taken, 2)

                    # Parse response based on provider
                    if "generativelanguage.googleapis.com" in AI_ENDPOINT:
                        if result and result.get('candidates') and result['candidates'][0].get('content') and result['candidates'][0]['content'].get('parts'):
                            ai_recommendations = result['candidates'][0]['content']['parts'][0]['text']
                            structured_data["ai_analysis"]["status"] = "success"
                            structured_data["ai_analysis"]["response_characters"] = len(ai_recommendations)
                        else:
                            ai_recommendations = f"AI response structure unexpected (Gemini): {json.dumps(result)}"
                            structured_data["ai_analysis"]["status"] = "error"
                            structured_data["ai_analysis"]["details"] = f"Unexpected response: {json.dumps(result)}"
                    else:
                        if result and result.get('choices') and result['choices'][0].get('message') and result['choices'][0]['message'].get('content'):
                            ai_recommendations = result['choices'][0]['message']['content']
                            structured_data["ai_analysis"]["status"] = "success"
                            structured_data["ai_analysis"]["response_characters"] = len(ai_recommendations)
                            if 'usage' in result:
                                structured_data["ai_analysis"]["prompt_tokens"] = result['usage'].get('prompt_tokens')
                                structured_data["ai_analysis"]["completion_tokens"] = result['usage'].get('completion_tokens')
                                structured_data["ai_analysis"]["total_tokens"] = result['usage'].get('total_tokens')
                        else:
                            ai_recommendations = f"AI response structure unexpected (OpenAI-compatible): {json.dumps(result)}"
                            structured_data["ai_analysis"]["status"] = "error"
                            structured_data["ai_analysis"]["details"] = f"Unexpected response: {json.dumps(result)}"

                except requests.exceptions.RequestException as e:
                    ai_recommendations = f"Error communicating with AI API: {e}"
                    structured_data["ai_analysis"]["status"] = "error"
                    structured_data["ai_analysis"]["details"] = str(e)
                except json.JSONDecodeError as e:
                    ai_recommendations = f"Error decoding AI response JSON: {e}. Response text: {response.text}"
                    structured_data["ai_analysis"]["status"] = "error"
                    structured_data["ai_analysis"]["details"] = f"JSON decode error: {e}, Response: {response.text}"
                except Exception as e:
                    ai_recommendations = f"An unexpected error occurred during AI processing: {e}"
                    structured_data["ai_analysis"]["status"] = "error"
                    structured_data["ai_analysis"]["details"] = str(e)
        else:
            # Offline mode
            ai_recommendations = '''[NOTE]
====
AI analysis is enabled but configured for offline processing. The enhanced dynamic prompt has been generated and saved to 'structured_health_check_findings.json'.

To get AI recommendations from this data:

1. **Ensure you have network access** to your chosen AI provider's API
2. **Use the enhanced prompt** from the `prompt_sent` field in the JSON
3. **Send this prompt to your AI API endpoint**
4. **Integrate the AI's response** into your report

The enhanced prompt includes dynamic severity analysis and context-aware guidance.
====
'''
            structured_data["ai_analysis"]["status"] = "offline_mode"
            structured_data["ai_analysis"]["note"] = "Enhanced AI analysis skipped for integrated run; dynamic prompt saved for offline processing."
    else:
        ai_recommendations = "[NOTE]\n====\nAI analysis is disabled in config.yaml (ai_analyze is false). No AI recommendations will be generated.\n====\n"
        structured_data["ai_analysis"]["status"] = "disabled"
        structured_data["ai_analysis"]["note"] = "AI analysis is disabled by configuration."

    structured_data["ai_analysis"]["recommendations_output"] = ai_recommendations

    # --- Step 3: Integrate AI Response into AsciiDoc Content ---
    adoc_content.append("\n==== Enhanced AI-Generated Recommendations\n")
    
    # Add AI analysis statistics
    adoc_content.append(f"[cols=\"1,1\",options=\"header\"]\n|===\n|Metric | Value\n|AI Endpoint | `{structured_data['ai_analysis'].get('endpoint', 'N/A')}`\n|AI Model | `{structured_data['ai_analysis'].get('model', 'N/A')}`\n|AI Temperature | `{structured_data['ai_analysis'].get('temperature', 'N/A')}`\n|AI Max Output Tokens | `{structured_data['ai_analysis'].get('max_output_tokens_requested', 'N/A')}`\n|Prompt Characters | `{structured_data['ai_analysis'].get('prompt_characters', 'N/A')}`\n|Response Characters | `{structured_data['ai_analysis'].get('response_characters', 'N/A')}`\n|Analysis Time | `{structured_data['ai_analysis'].get('analysis_time_seconds', 'N/A')}` seconds\n")
    
    if structured_data['ai_analysis'].get('prompt_tokens') is not None:
        adoc_content.append(f"|Prompt Tokens | `{structured_data['ai_analysis'].get('prompt_tokens', 'N/A')}`\n")
        adoc_content.append(f"|Completion Tokens | `{structured_data['ai_analysis'].get('completion_tokens', 'N/A')}`\n")
        adoc_content.append(f"|Total Tokens | `{structured_data['ai_analysis'].get('total_tokens', 'N/A')}`\n")
    
    adoc_content.append("|===\n\n")

    # Add the AI recommendations
    adoc_content.append(ai_recommendations)
    
    # Add dynamic analysis insights
    if structured_data.get("dynamic_analysis") and isinstance(structured_data["dynamic_analysis"], dict):
        if "critical_issues" in structured_data["dynamic_analysis"] and structured_data["dynamic_analysis"]["critical_issues"]:
            adoc_content.append("\n==== Dynamic Analysis Insights ===\n")
            adoc_content.append("The following critical issues were identified and prioritized in the AI prompt:\n\n")
            
            for issue in structured_data["dynamic_analysis"]["critical_issues"]:
                adoc_content.append(f"**{issue['metric'].replace('_', ' ').title()}**\n")
                adoc_content.append(f"- Severity: {issue['analysis']['level'].upper()}\n")
                adoc_content.append(f"- Analysis: {issue['analysis']['reasoning']}\n")
                if issue['analysis']['recommendations']:
                    adoc_content.append(f"- Quick Actions: {', '.join(issue['analysis']['recommendations'])}\n")
                adoc_content.append("\n")

    adoc_content.append("[TIP]\n====\n")
    adoc_content.append("This enhanced AI analysis uses dynamic prompt generation based on metric severity analysis. ")
    adoc_content.append("Critical issues are automatically prioritized and highlighted in the AI prompt, ")
    adoc_content.append("resulting in more focused and actionable recommendations.\n")
    adoc_content.append("====\n")

    return "\n".join(adoc_content), structured_data 