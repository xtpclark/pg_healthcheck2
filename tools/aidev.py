#!/usr/bin/env python3
"""
AI Developer Assistant for the Health Check Framework.

This script provides an interactive command-line interface to generate
prompts for scaffolding new plugins or adding new checks. It can either
display the prompt for manual use or execute it directly against a
configured AI service to generate a ready-to-use shell script.
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

def get_input(prompt, default=None):
    """Gets user input with an optional default value and strips whitespace."""
    if default:
        return (input(f"{prompt} [{default}]: ").strip() or default)
    return input(f"{prompt}: ").strip()

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

def execute_ai_prompt(prompt, settings):
    """Sends the generated prompt to the configured AI service."""
    if not settings:
        print("❌ AI settings not loaded. Cannot execute.")
        return None

    ai_provider = settings.get('ai_provider', 'openai')
    API_ENDPOINT = settings.get('ai_endpoint')
    AI_MODEL = settings.get('ai_model')
    API_KEY = settings.get('ai_api_key')

    if not all([API_ENDPOINT, AI_MODEL, API_KEY]):
        print("❌ AI configuration (`ai_endpoint`, `ai_model`, `ai_api_key`) is incomplete in your config file.")
        return None

    print(f"\n--- Executing AI Code Generation ---")
    print(f"  - Provider: {ai_provider}")
    print(f"  - Model: {AI_MODEL}")
    
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

# --- Main Functions for Sub-commands ---

def handle_prompt_workflow(prompt, settings):
    """Handles the interactive part of the workflow after a prompt is generated."""
    while True:
        choice = input("\nChoose an action: [P]rint prompt, [E]xecute with AI, [Q]uit: ").lower()
        if choice == 'p':
            print("\n" + "="*80)
            print("✅ Prompt Generated! Copy everything below and paste it into your AI assistant.")
            print("="*80 + "\n")
            print(prompt)
            break
        elif choice == 'e':
            generated_script = execute_ai_prompt(prompt, settings)
            if generated_script:
                # Clean up potential markdown code fences from the AI response
                if generated_script.strip().startswith("```bash"):
                    generated_script = generated_script.strip()[7:-4]
                elif generated_script.strip().startswith("```"):
                     generated_script = generated_script.strip()[3:-3]

                print("\n" + "="*80)
                print("✅ AI Generation Complete! Copy the shell script below and run it in your terminal.")
                print("="*80 + "\n")
                print(generated_script.strip())
            break
        elif choice == 'q':
            print("Aborted.")
            break
        else:
            print("Invalid choice, please try again.")


def scaffold_plugin(settings):
    """Guides the user through creating a prompt to scaffold a new plugin."""
    print("\n--- Scaffolding a New Health Check Plugin ---")
    print("Please provide the following details about the new technology.")

    details = {
        'technology_name': get_input("Technology Name (e.g., Cassandra, MySQL)"),
        'lowercase_name': get_input("Lowercase name for code (e.g., cassandra, mysql)"),
        'connection_library': get_input("Main Python connection library (e.g., cassandra-driver)"),
        'connection_class': get_input("Primary connection class/function (e.g., cassandra.cluster.Cluster)"),
        'connection_param': get_input("Key connection parameter (e.g., contact_points)"),
        'version_query': get_input("Example 'Get Version' query/command", "SELECT 'version'"),
    }

    prompt = render_prompt("plugin_scaffold_prompt.adoc", details)
    handle_prompt_workflow(prompt, settings)


def add_check(settings):
    """Guides the user through creating a prompt to add a new check."""
    print("\n--- Adding a New Check to an Existing Plugin (Scaffolder) ---")
    print("Please provide the following details about the new check.")

    details = {
        'plugin_name': get_input("Lowercase name of the existing plugin (e.g., kafka, postgres)"),
        'check_filename': get_input("New check filename (e.g., topic_health_check.py)"),
        'check_function_name': get_input("New check function name (e.g., run_topic_health_check)"),
        'check_description': get_input("Brief description of what this check does"),
        'check_query': get_input("Example query/command for the check", "SELECT 'example'"),
        'report_title': get_input("Title for this check in the final report", "New Check Title"),
    }
    
    prompt = render_prompt("add_check_prompt.adoc", details)
    handle_prompt_workflow(prompt, settings)

def generate_check(settings):
    """Guides the user through a natural language prompt to generate a new check."""
    print("\n--- Generating a New Check in an Existing Plugin (AI Generator) ---")
    print("Describe the check you want to create in plain English.")

    details = {
        'plugin_name': get_input("Lowercase name of the existing plugin (e.g., kafka, postgres)"),
        'natural_language_request': get_input("Describe the check (e.g., 'Check for Kafka topics with a low replication factor')")
    }
    
    prompt = render_prompt("generate_check_prompt.adoc", details)
    handle_prompt_workflow(prompt, settings)

def add_report(settings):
    """Guides the user through creating a prompt to add a new report definition."""
    print("\n--- Adding a New Report Definition to an Existing Plugin ---")
    print("Please provide the following details for the new report.")

    details = {
        'plugin_name': get_input("Lowercase name of the existing plugin (e.g., postgres)"),
        'report_filename': get_input("New report filename (e.g., security_report.py)"),
        'report_function_name': get_input("New report definition function name (e.g., get_security_report_definition)"),
    }
    
    prompt = render_prompt("add_report_prompt.adoc", details)
    handle_prompt_workflow(prompt, settings)


def main():
    """Main entry point with command-line argument parsing."""
    parser = argparse.ArgumentParser(description='AI Developer Assistant for Health Check Plugins.')
    parser.add_argument('--config', default='config/config.yaml', help='Path to the main configuration file')
    subparsers = parser.add_subparsers(dest='command', required=True, help='Available commands')

    # --- Scaffolding Commands ---
    parser_scaffold = subparsers.add_parser('scaffold-plugin', help='Scaffold a new plugin skeleton from detailed prompts.')
    parser_scaffold.set_defaults(func=scaffold_plugin)

    parser_add = subparsers.add_parser('add-check', help='Scaffold a new check in an existing plugin from detailed prompts.')
    parser_add.set_defaults(func=add_check)

    parser_add_report = subparsers.add_parser('add-report', help='Scaffold a new report definition in an existing plugin.')
    parser_add_report.set_defaults(func=add_report)
    
    # --- Generative Commands ---
    parser_generate_check = subparsers.add_parser('generate-check', help='Generate a new check from a natural language description.')
    parser_generate_check.set_defaults(func=generate_check)


    args = parser.parse_args()
    
    # Load the main configuration to get AI settings
    settings = load_config(args.config)
    
    # Call the selected function, passing in the settings
    args.func(settings)

if __name__ == '__main__':
    main()
