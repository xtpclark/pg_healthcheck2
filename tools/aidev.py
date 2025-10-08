#!/usr/bin/env python3
"""
AI Developer Agent for the Health Check Framework.

This script acts as a conversational agent that understands natural language
requests to scaffold, generate, and integrate code for the framework. It also
validates and self-corrects the code it produces.
"""
import argparse
from pathlib import Path
import yaml
import json
import requests
import time
import sys
import jinja2

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

def validate_and_correct_code(file_path, settings):
    """Lints a Python file and triggers a self-correction loop if errors are found."""
    if not pyflakes_check:
        print("  - [WARN] Pyflakes not found, skipping code validation.")
        return

    print(f"  - Validating syntax for: {file_path}")
    try:
        code_to_check = Path(file_path).read_text(encoding='utf-8')
        reporter = PyflakesReporter()
        pyflakes_check(code_to_check, str(file_path), reporter)

        if reporter.errors:
            print(f"  - [WARN] Found {len(reporter.errors)} issues. Attempting self-correction...")
            error_string = "\n".join(reporter.errors)
            
            corrector_prompt = render_prompt("code_corrector_prompt.adoc", {
                "original_code": code_to_check,
                "linter_errors": error_string
            })

            corrected_code_raw = execute_ai_prompt(corrector_prompt, settings)
            if not corrected_code_raw:
                raise ValueError("AI failed to provide a correction.")

            corrected_code = clean_ai_response(corrected_code_raw, "python")
            Path(file_path).write_text(corrected_code, encoding='utf-8')
            print("  - ✅ Self-correction applied successfully. Re-validating...")
            
            final_reporter = PyflakesReporter()
            pyflakes_check(corrected_code, str(file_path), final_reporter)
            if final_reporter.errors:
                print(f"  - [ERROR] Self-correction failed. {len(final_reporter.errors)} issues remain.")
            else:
                print("  - ✅ Code is now valid.")
        else:
            print("  - ✅ Code is valid.")

    except Exception as e:
        print(f"❌ An error occurred during code validation: {e}")

def execute_operations(operations, settings):
    """Parses and executes file creation operations, with validation."""
    if not operations:
        print("⚠️ AI did not provide any file operations to execute.")
        return False
    print("\n--- Executing File Creation Plan ---")
    for op in operations:
        # --- NEW: Hardened defensive check ---
        action = op.get("action")
        path_str = op.get("path") or op.get("target_file") 
        if not all([action, path_str]):
            print(f"⚠️  Skipping malformed operation in AI plan: {op}")
            continue
        # --- End of new check ---

        path = Path(path_str)
        try:
            if action == "create_file":
                print(f"  - Writing file: {path}")
                content = op.get("content", "")
                if isinstance(content, list):
                    content = "\n".join(str(line) for line in content)

                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding='utf-8')
                
                if path.suffix == '.py':
                    validate_and_correct_code(path, settings)
            else:
                print(f"⚠️ Unknown action '{action}' requested. Skipping.")
        except Exception as e:
            print(f"❌ Failed to execute action '{action}' on '{path}': {e}")
            return False
    print("✅ File creation plan executed successfully.")
    return True

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
