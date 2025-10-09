#!/usr/bin/env python3
"""
AI Developer Agent for the Health Check Framework.

This script acts as a conversational agent that understands natural language
requests to scaffold, generate, and integrate code for the framework. It also
validates and self-corrects the code it produces.
"""

import os
import argparse
from pathlib import Path
import yaml
import json
import requests
import time
import sys
import jinja2
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f'logs/aidev_{datetime.now():%Y%m%d_%H%M%S}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
	
# --- Pyflakes import for self-correction ---
try:
    from pyflakes.api import check as pyflakes_check
    from pyflakes.reporter import Reporter
except ImportError:
    print("❌ Pyflakes is not installed. Self-correction feature will be disabled.")
    print("   Please run: pip install pyflakes")
    pyflakes_check = None

# --- Helper class to capture linter errors ---
class PyflakesReporter(Reporter):
    """Custom reporter to capture pyflakes errors as a list of strings."""
    def __init__(self):
        self.errors = []
    def unexpectedError(self, filename, msg):
        self.errors.append(f"Unexpected Error: {msg}")
    def syntaxError(self, filename, msg, lineno, offset, text):
        self.errors.append(f"Syntax Error at line {lineno}: {msg}")
    def flake(self, message):
        self.errors.append(str(message))

# --- Helper Functions ---
def render_prompt(template_name, context):
    """Loads and renders a Jinja2 prompt template."""
    template_dir = Path(__file__).parent / "templates"
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(template_dir)), trim_blocks=True, lstrip_blocks=True)
    try:
        template = env.get_template(template_name)
        return template.render(context)
    except jinja2.exceptions.TemplateNotFound:
        print(f"❌ Error: Prompt template not found at {template_dir / template_name}"); exit(1)

def load_config(config_path):
    """Loads the main YAML configuration file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"⚠️  Warning: Main config file not found at '{config_path}'."); return None
    except yaml.YAMLError as e:
        print(f"❌ Error loading settings from {config_path}: {e}"); return None

def execute_ai_prompt(prompt, settings, model_override=None):
    """
    Sends the generated prompt to the configured AI service with corporate proxy support.
    
    Args:
        prompt: The prompt string to send
        settings: Settings dict with AI configuration
        model_override: Optional model name to override settings
        
    Returns:
        str: The AI response text, or None on error
    """
    if not settings:
        print("❌ AI settings not loaded.")
        return None
    
    ai_provider = settings.get('ai_provider', 'openai')
    API_ENDPOINT = settings.get('ai_endpoint')
    AI_MODEL = model_override or settings.get('ai_model')
    API_KEY = settings.get('ai_api_key', '')
    
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
        AI_TEMPERATURE = settings.get('ai_temperature', 0.7)
        AI_MAX_OUTPUT_TOKENS = settings.get('ai_max_output_tokens', 8192)  # Higher for code generation
        
        headers = {'Content-Type': 'application/json'}
        
        # Corporate proxy settings
        AI_USER = settings.get('ai_user', 'anonymous')
        AI_USER_HEADER = settings.get('ai_user_header', '')
        SSL_CERT_PATH = settings.get('ssl_cert_path', '')
        AI_SSL_VERIFY = settings.get('ai_ssl_verify', True)
        
        # Add custom user header if configured
        if AI_USER_HEADER:
            headers[AI_USER_HEADER] = AI_USER
        
        # Handle SSL verification
        verify_ssl = AI_SSL_VERIFY
        if verify_ssl and SSL_CERT_PATH:
            verify_ssl = SSL_CERT_PATH
        
        # Build provider-specific request
        if "generativelanguage.googleapis.com" in API_ENDPOINT:
            # Google Generative AI format
            API_URL = f"{API_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}"
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": AI_TEMPERATURE,
                    "maxOutputTokens": AI_MAX_OUTPUT_TOKENS
                }
            }
        else:
            # OpenAI-compatible format (includes xAI/Grok)
            API_URL = f"{API_ENDPOINT}v1/chat/completions"
            headers['Authorization'] = f'Bearer {API_KEY}'
            payload = {
                "model": AI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": AI_TEMPERATURE,
                "max_tokens": AI_MAX_OUTPUT_TOKENS,
                "user": AI_USER
            }
        
        start_time = time.time()
        
        # Make the API request with timeout
        response = requests.post(
            API_URL,
            headers=headers,
            data=json.dumps(payload),
            verify=verify_ssl,
            timeout=settings.get('ai_timeout', 300)  # 5 minute default
        )
        response.raise_for_status()
        result = response.json()
        
        duration = time.time() - start_time
        print(f"  - AI Processing Time: {duration:.2f} seconds")
        
        # Parse provider-specific response
        if "generativelanguage.googleapis.com" in API_ENDPOINT:
            return result['candidates'][0]['content']['parts'][0]['text']
        else:
            return result['choices'][0]['message']['content']
        
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


def OLD_execute_ai_prompt(prompt, settings, model_override=None):
    """Sends the generated prompt to the configured AI service."""
    if not settings:
        print("❌ AI settings not loaded."); return None
    ai_provider = settings.get('ai_provider', 'openai')
    API_ENDPOINT = settings.get('ai_endpoint')
    AI_MODEL = model_override or settings.get('ai_model')
    API_KEY = settings.get('ai_api_key')
    if not all([API_ENDPOINT, AI_MODEL, API_KEY]):
        print("❌ AI configuration is incomplete."); return None
    print(f"  - Contacting AI ({AI_MODEL})...")
    headers = {'Content-Type': 'application/json'}
    payload = {}
    try:
        if "generativelanguage.googleapis.com" in API_ENDPOINT:
            API_URL = f"{API_ENDPOINT}{AI_MODEL}:generateContent?key={API_KEY}"
            payload = {"contents": [{"parts": [{"text": prompt}]}]}
        else: # OpenAI-compatible
            API_URL = f"{API_ENDPOINT}/v1/chat/completions"
            headers['Authorization'] = f'Bearer {API_KEY}'
            payload = { "model": AI_MODEL, "messages": [{"role": "user", "content": prompt}] }
        start_time = time.time()
        response = requests.post(API_URL, headers=headers, data=json.dumps(payload), timeout=180)
        response.raise_for_status()
        result = response.json()
        duration = time.time() - start_time
        print(f"  - AI Processing Time: {duration:.2f} seconds")
        if "generativelanguage.googleapis.com" in API_ENDPOINT:
            return result['candidates'][0]['content']['parts'][0]['text']
        else:
            return result['choices'][0]['message']['content']
    except Exception as e:
        print(f"❌ An error occurred during the AI request: {e}"); return None

def clean_ai_response(response_text, response_type="json"):
    """Cleans markdown fences from AI responses."""
    fence_map = { "json": "```json", "python": "```python" }
    fence = fence_map.get(response_type, "```")
    if fence in response_text:
        return response_text.split(fence)[1].split("```")[0].strip()
    if "*AI JSON Response:*" in response_text:
        return response_text.split("*AI JSON Response:*")[1].strip()
    return response_text.strip()

def validate_and_correct_code(file_path, settings, max_attempts=3):
    """Validates and corrects code with retry limits and rollback."""
    original_code = Path(file_path).read_text(encoding='utf-8')
    current_code = original_code
    
    for attempt in range(max_attempts):
        reporter = PyflakesReporter()
        pyflakes_check(current_code, str(file_path), reporter)
        
        if not reporter.errors:
            # Success! Write if we corrected it
            if attempt > 0:
                Path(file_path).write_text(current_code, encoding='utf-8')
                print(f"  - ✅ Code validated after {attempt} correction(s)")
            else:
                print("  - ✅ Code is valid")
            return True
        
        # Still has errors
        if attempt == max_attempts - 1:
            # Final attempt failed - rollback to original
            print(f"  - ❌ Self-correction failed after {max_attempts} attempts. Rolling back.")
            Path(file_path).write_text(original_code, encoding='utf-8')
            return False
        
        # Try to correct
        print(f"  - Attempt {attempt + 1}/{max_attempts}: Found {len(reporter.errors)} issues. Correcting...")
        error_string = "\n".join(reporter.errors)
        
        corrector_prompt = render_prompt("code_corrector_prompt.adoc", {
            "original_code": current_code,
            "linter_errors": error_string
        })
        
        corrected_code_raw = execute_ai_prompt(corrector_prompt, settings)
        if not corrected_code_raw:
            print("  - ⚠️ AI failed to provide correction. Rolling back.")
            Path(file_path).write_text(original_code, encoding='utf-8')
            return False
        
        current_code = clean_ai_response(corrected_code_raw, "python")
    
    return False

def execute_operations(operations, settings):
    """Executes operations with rollback on failure."""
    if not operations:
        print("⚠️ AI did not provide any file operations to execute.")
        return False
    
    print("\n--- Executing File Creation Plan ---")
    created_paths = []
    
    # Change to project root for file operations
    original_dir = Path.cwd()
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)
    
    try:
        for op in operations:
            action = op.get("action")
            path_str = op.get("path") or op.get("target_file")
            if not all([action, path_str]):
                print(f"⚠️ Skipping malformed operation: {op}")
                continue
            path = Path(path_str)
            if action == "create_file":
                print(f"  - Writing file: {path}")
                content = op.get("content", "")
                if isinstance(content, list):
                    content = "\n".join(str(line) for line in content)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding='utf-8')
                created_paths.append(path)
                # Validate Python files
                if path.suffix == '.py':
                    if not validate_and_correct_code(path, settings):
                        raise ValueError(f"Failed to validate: {path}")
            elif action == "create_directory":
                print(f"  - Creating directory: {path}")
                path.mkdir(parents=True, exist_ok=True)
                created_paths.append(path)
            else:
                print(f"⚠️ Unknown action '{action}'. Skipping.")
        print("✅ File creation plan executed successfully.")
        return True
    except Exception as e:
        print(f"\n❌ Operation failed: {e}")
        print("🔄 Rolling back all changes...")
        for path in reversed(created_paths):
            try:
                if path.is_file():
                    path.unlink()
                    print(f"  - Removed file: {path}")
                elif path.is_dir() and not any(path.iterdir()):
                    path.rmdir()
                    print(f"  - Removed empty directory: {path}")
            except Exception as cleanup_error:
                print(f"  - ⚠️ Could not remove {path}: {cleanup_error}")
        print("❌ All changes rolled back.")
        return False
    finally:
        # Always return to original directory
        os.chdir(original_dir)

def handle_code_integration(integration_step, settings):
    """Performs the Read-Modify-Write operation, creating a stub file if needed."""
    if not integration_step: return
    target_file_hint = integration_step.get("target_file_hint")
    if not target_file_hint:
        print("⚠️  Integration step is missing the 'target_file_hint'. Skipping."); return
    target_file = Path(target_file_hint)
    instruction = integration_step.get("instruction")
    code_to_add = integration_step.get("code_snippet_to_add")
    if not all([instruction, code_to_add]):
        print("⚠️  Integration step is incomplete. Skipping."); return
    print("\n" + "="*50)
    print(f"💡 The AI has suggested an integration step for {target_file}")
    choice = input("Shall I attempt to apply this change automatically? (Y/n) > ").lower().strip()
    if choice != 'n':
        try:
            original_code = ""
            try:
                original_code = target_file.read_text(encoding='utf-8')
            except FileNotFoundError:
                print(f"  - [INFO] Target file '{target_file}' not found. Creating a default stub file.")
                func_name = f"get_{target_file.stem}_report_definition"
                stub_content = (
                    'REPORT_SECTIONS = [\n    {\n        "title": "Default Section",\n        "actions": []\n    }\n]\n\n'
                    f'def {func_name}(connector, settings):\n    """Returns the report structure."""\n    return REPORT_SECTIONS\n'
                )
                target_file.parent.mkdir(parents=True, exist_ok=True)
                target_file.write_text(stub_content, encoding='utf-8')
                original_code = stub_content
            modifier_prompt = render_prompt("code_modifier_prompt.adoc", {
                "original_code": original_code,
                "modification_instruction": f"{instruction}\n\nHere is the dictionary to add:\n{code_to_add}"
            })
            print("  - Asking AI to perform the code modification...")
            modified_code_raw = execute_ai_prompt(modifier_prompt, settings)
            if not modified_code_raw: raise ValueError("AI did not return any modified code.")
            modified_code = clean_ai_response(modified_code_raw, "python")
            target_file.write_text(modified_code, encoding='utf-8')
            print("✅ Code integration successful!")
        except Exception as e:
            print(f"❌ An error occurred during automatic code integration: {e}")

def execute_plan_from_ai(plan_response_raw, settings):
    """Parses and executes a plan, including the new integration step."""
    if not plan_response_raw:
        print("❌ AI failed to generate an execution plan.")
        return

    try:
        plan_json = json.loads(clean_ai_response(plan_response_raw, "json"))
        operations, integration_step = plan_json.get("operations"), plan_json.get("integration_step")
        if execute_operations(operations, settings):
            handle_code_integration(integration_step, settings)
    except Exception as e: # More specific error handling
        print(f"❌ Failed to process AI's execution plan. Error: {e}\nRaw Response: {plan_response_raw}")
        return

def sanitize_user_input(user_query):
    """Basic input validation."""
    if len(user_query) > 1000:
        raise ValueError("Query too long (max 1000 characters)")
    
    # Check for obvious injection patterns
    dangerous = ['rm -rf', 'DROP TABLE', '; --', '$(', '`']
    for pattern in dangerous:
        if pattern in user_query:
            raise ValueError(f"Query contains suspicious pattern: {pattern}")
    
    return user_query.strip()

def recognize_intent_and_dispatch(user_query, settings):
    """Dispatches user query to the correct handler."""
    print(f"\n🤔 Understanding your request: \"{user_query}\"")
    intent_prompt = render_prompt("intent_recognizer_prompt.adoc", {"user_query": user_query})
    intent_response_raw = execute_ai_prompt(intent_prompt, settings)
    if not intent_response_raw: return
    try:
        intent_json = json.loads(clean_ai_response(intent_response_raw))
        intent, entities = intent_json.get("intent"), intent_json.get("entities")
        print(f"  - Intent recognized: '{intent}'")
        handler_map = {
            "generate-check": handle_generate_check, "generate-rule": handle_generate_rule,
            "scaffold-plugin": handle_scaffold_plugin, "add-report": handle_add_report,
            "add-check": handle_add_check, "plan_comprehensive_checks": handle_plan_comprehensive_checks
        }
        handler = handler_map.get(intent)
        if handler:
            if intent == "plan_comprehensive_checks":
                handler(entities, settings, user_query)
            else:
                handler(entities, settings)
        else:
            print(f"⚠️  Sorry, I don't know how to handle the intent '{intent}'.")
    except Exception as e:
        print(f"❌ Failed to parse AI's intent response. Error: {e}")

def generic_handler(task_name, prompt_template, entities, settings):
    """A generic handler for simple, single-prompt tasks."""
    print(f"\n💡 {task_name}...")
    action_prompt = render_prompt(prompt_template, entities)
    plan_response_raw = execute_ai_prompt(action_prompt, settings)
    execute_plan_from_ai(plan_response_raw, settings)

def handle_generate_check(entities, settings):
    generic_handler("Generating new check", "generate_check_prompt.adoc", entities, settings)

def handle_generate_rule(entities, settings):
    generic_handler("Generating new JSON rule", "generate_rule_prompt.adoc", entities, settings)
	
def handle_scaffold_plugin(entities, settings):
    """Prepares variables and calls the generic handler for scaffolding."""
    tech_name = entities.get("technology_name", "UnknownPlugin")
    entities['technology_name_lowercase'] = tech_name.lower().replace(' ', '').replace('-', '')
    entities['TechnologyNameCamelCase'] = tech_name.replace(' ', '').replace('-', '').title()
    generic_handler("Scaffolding new plugin", "plugin_scaffold_prompt.adoc", entities, settings)

def handle_add_report(entities, settings):
    generic_handler("Adding new report", "add_report_prompt.adoc", entities, settings)

def handle_add_check(entities, settings):
    generic_handler("Adding new boilerplate check", "add_check_prompt.adoc", entities, settings)

def handle_plan_comprehensive_checks(entities, settings, user_query):
    """Handles the multi-step planner/executor workflow."""
    print("\n💡 Formulating a comprehensive plan...")
    planner_prompt = render_prompt("planner_prompt.adoc", {"user_query": user_query})
    plan_response_raw = execute_ai_prompt(planner_prompt, settings)
    if not plan_response_raw: return
    try:
        plan_json = json.loads(clean_ai_response(plan_response_raw))
        tasks = plan_json.get("plan", [])
        if not tasks: print("⚠️ AI returned an empty plan."); return
        print("\n🤖 I have formulated a plan to create the following solutions:")
        for i, task in enumerate(tasks, 1):
            print(f"   {i}. {task}")
        mode = 'ask'
        for i, task in enumerate(tasks, 1):
            print("\n" + "-"*50)
            print(f"Next up: \"{task}\" ({i}/{len(tasks)})")
            if mode != 'all':
                choice = input("Generate this solution? (Y)es / (n)o / (a)ll / (q)uit > ").lower().strip()
                if choice == 'q': print("Aborting plan execution."); return
                elif choice == 'n': print("Skipping..."); continue
                elif choice == 'a': mode = 'all'
            tech_name = entities.get("technology_name", "").lower()
            if not tech_name and "postgres" in user_query: tech_name = "postgres"
            execution_query = f"add a {tech_name} check for {task}"
            recognize_intent_and_dispatch(execution_query, settings)
        print("\n✅ Comprehensive plan executed successfully.")
    except Exception as e:
        print(f"❌ Failed to parse AI's execution plan. Error: {e}\nRaw Response: {plan_response_raw}")

def main():
    """Main entry point with a new conversational interface."""
    parser = argparse.ArgumentParser(description='AI Developer Agent.')
    parser.add_argument('query', nargs='?', default=None, help='Your development request in natural language.')
    parser.add_argument('--config', default='config/config.yaml', help='Path to the main configuration file')
    args = parser.parse_args()
    settings = load_config(args.config)
    if not settings: print("Please ensure your config/config.yaml is set up correctly."); return
    if args.query:
        recognize_intent_and_dispatch(args.query, settings)
    else:
        print("🤖 AI Developer Agent is ready.")
        print("   (e.g., 'generate a comprehensive set of postgres health checks')")
        print("   Type 'quit' or 'exit' to end the session.")
        while True:
            try:
                user_input = input("> ")
                if user_input.lower() in ['quit', 'exit']: break
                if user_input: recognize_intent_and_dispatch(user_input, settings)
            except KeyboardInterrupt:
                break
        print("Goodbye!")

if __name__ == '__main__':
    main()
