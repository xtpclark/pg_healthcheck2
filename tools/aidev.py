#!/usr/bin/env python3
"""
AI Developer Agent for the Health Check Framework.

This script acts as a conversational agent that understands natural language
requests to scaffold and generate code for the framework. It formulates a plan
of action and executes it directly on the filesystem.
"""
import argparse
from pathlib import Path
import yaml
import json
import requests
import time
import sys
import jinja2

# --- Helper Functions ---

def render_prompt(template_name, context):
    """Loads and renders a Jinja2 prompt template."""
    template_dir = Path(__file__).parent / "templates"
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(template_dir)), trim_blocks=True, lstrip_blocks=True)
    try:
        template = env.get_template(template_name)
        return template.render(context)
    except jinja2.exceptions.TemplateNotFound:
        print(f"❌ Error: Prompt template not found at {template_dir / template_name}")
        exit(1)

def load_config(config_path):
    """Loads the main YAML configuration file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"⚠️  Warning: Main config file not found at '{config_path}'. Cannot execute AI prompts.")
        return None
    except yaml.YAMLError as e:
        print(f"❌ Error loading settings from {config_path}: {e}")
        return None

# --- AI Execution Logic ---

def execute_ai_prompt(prompt, settings, model_override=None):
    """Sends the generated prompt to the configured AI service."""
    if not settings:
        print("❌ AI settings not loaded. Cannot execute.")
        return None

    ai_provider = settings.get('ai_provider', 'openai')
    API_ENDPOINT = settings.get('ai_endpoint')
    AI_MODEL = model_override or settings.get('ai_model')
    API_KEY = settings.get('ai_api_key')

    if not all([API_ENDPOINT, AI_MODEL, API_KEY]):
        print("❌ AI configuration (`ai_endpoint`, `ai_model`, `ai_api_key`) is incomplete in your config file.")
        return None

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
        print(f"❌ An error occurred during the AI request: {e}")
        return None

# --- Core Agent Logic ---

def execute_operations(operations):
    """
    Parses a list of JSON operations from the AI and executes them.
    """
    if not operations:
        print("⚠️ AI did not provide any operations to execute.")
        return

    print("\n--- Executing Plan ---")
    for op in operations:
        action = op.get("action")
        path = op.get("path")
        
        try:
            if action == "create_directory":
                print(f"  - Creating directory: {path}")
                Path(path).mkdir(parents=True, exist_ok=True)
            elif action == "create_file":
                print(f"  - Writing file: {path}")
                content = op.get("content", "")
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                Path(path).write_text(content, encoding='utf-8')
            else:
                print(f"⚠️ Unknown action '{action}' requested by AI. Skipping.")
        except Exception as e:
            print(f"❌ Failed to execute action '{action}' on '{path}': {e}")
            return
    print("✅ Plan executed successfully.")

def clean_ai_json_response(response_text):
    """Cleans markdown fences and extracts the JSON object."""
    if "```json" in response_text:
        return response_text.split("```json")[1].split("```")[0].strip()
    return response_text.strip()
    
def execute_plan_from_ai(plan_response_raw):
    """Parses and executes a plan from a raw AI response."""
    if not plan_response_raw:
        print("❌ AI failed to generate an execution plan.")
        return

    try:
        plan_json = json.loads(clean_ai_json_response(plan_response_raw))
        operations = plan_json.get("operations")
        post_message = plan_json.get("post_message")
        
        execute_operations(operations)
        
        if post_message:
            print("\n" + "="*50 + "\n📝 AI Message:\n" + post_message + "\n" + "="*50)
            
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"❌ Failed to parse AI's execution plan. Error: {e}\nRaw Response: {plan_response_raw}")
        return

def recognize_intent_and_dispatch(user_query, settings):
    """
    Takes the user's query, uses the AI to recognize the intent and entities,
    and then dispatches to the correct handler function.
    """
    print(f"\n🤔 Understanding your request: \"{user_query}\"")
    
    intent_prompt = render_prompt("intent_recognizer_prompt.adoc", {"user_query": user_query})
    intent_response_raw = execute_ai_prompt(intent_prompt, settings)
    
    if not intent_response_raw:
        print("❌ Could not get a response from the AI for intent recognition.")
        return
        
    try:
        intent_json = json.loads(clean_ai_json_response(intent_response_raw))
        intent = intent_json.get("intent")
        entities = intent_json.get("entities")
        
        print(f"  - Intent recognized: '{intent}'")
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"❌ Failed to parse AI's intent response. Error: {e}")
        print(f"Raw Response: {intent_response_raw}")
        return

    # Dispatch to the correct handler based on intent
    handler_map = {
        "generate-check": handle_generate_check,
        "generate-rule": handle_generate_rule,
        "scaffold-plugin": handle_scaffold_plugin,
        "add-report": handle_add_report,
        "add-check": handle_add_check,
        "plan_comprehensive_checks": handle_plan_comprehensive_checks
    }
    
    handler = handler_map.get(intent)
    if handler:
        handler(entities, settings)
    else:
        print(f"⚠️  Sorry, I don't know how to handle the intent '{intent}'.")

def generic_handler(task_name, prompt_template, entities, settings):
    """A generic handler to reduce code duplication."""
    print(f"\n💡 {task_name}...")
    action_prompt = render_prompt(prompt_template, entities)
    plan_response_raw = execute_ai_prompt(action_prompt, settings)
    execute_plan_from_ai(plan_response_raw)

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

def handle_plan_comprehensive_checks(entities, settings):
    """Handles the multi-step planner/executor workflow."""
    print("\n💡 Formulating a comprehensive plan...")
    
    # Step 1: Call the planner prompt
    planner_prompt = render_prompt("planner_prompt.adoc", entities)
    plan_response_raw = execute_ai_prompt(planner_prompt, settings)

    if not plan_response_raw:
        print("❌ AI failed to generate a plan.")
        return

    try:
        plan_json = json.loads(clean_ai_json_response(plan_response_raw))
        tasks = plan_json.get("plan", [])
        if not tasks:
            print("⚠️ AI returned an empty plan.")
            return

        # Step 2: Present the plan for user confirmation
        print("\n🤖 I have formulated a plan to create the following solutions:")
        for i, task in enumerate(tasks, 1):
            print(f"   {i}. {task}")

        # Step 3: Interactive Execution Loop
        mode = 'ask' # Modes: 'ask', 'all'
        for i, task in enumerate(tasks, 1):
            print("\n" + "-"*50)
            print(f"Next up: \"{task}\" ({i}/{len(tasks)})")

            if mode != 'all':
                choice = input("Generate this solution? (Y)es / (n)o / (a)ll / (q)uit > ").lower().strip()
                if choice == 'q':
                    print("Aborting plan execution.")
                    return
                elif choice == 'n':
                    print("Skipping...")
                    continue
                elif choice == 'a':
                    mode = 'all'
            
            # Use the existing `generate-check` intent to execute each task
            tech_name = entities.get("technology_name", "").lower()
            execution_query = f"add a {tech_name} check for {task}"
            recognize_intent_and_dispatch(execution_query, settings)

        print("\n✅ Comprehensive plan executed successfully.")

    except (json.JSONDecodeError, AttributeError) as e:
        print(f"❌ Failed to parse AI's execution plan. Error: {e}\nRaw Response: {plan_response_raw}")
        return

# --- Main Entry Point ---

def main():
    """Main entry point with a new conversational interface."""
    parser = argparse.ArgumentParser(description='AI Developer Agent for Health Check Plugins.')
    parser.add_argument('query', nargs='?', default=None, help='Your development request in natural language.')
    parser.add_argument('--config', default='config/config.yaml', help='Path to the main configuration file')
    args = parser.parse_args()

    settings = load_config(args.config)
    if not settings:
        print("Please ensure your config/config.yaml is set up correctly.")
        return

    if args.query:
        recognize_intent_and_dispatch(args.query, settings)
    else:
        print("🤖 AI Developer Agent is ready. What would you like to do?")
        print("   (e.g., 'generate a comprehensive set of postgres health checks')")
        print("   Type 'quit' or 'exit' to end the session.")
        while True:
            try:
                user_input = input("> ")
                if user_input.lower() in ['quit', 'exit']:
                    print("Goodbye!")
                    break
                if user_input:
                    recognize_intent_and_dispatch(user_input, settings)
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break

if __name__ == '__main__':
    main()
