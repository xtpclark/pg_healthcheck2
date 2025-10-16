# lib/generators/check.py
"""Check generation handler."""
from lib.intent import generic_handler

def old_handle_generate_check(entities, settings):
    """Generates a new check module."""
    generic_handler("Generating new check", "generate_check_prompt.adoc", entities, settings)


def handle_generate_check(intent_result, settings):
    """
    Generates a health check with technology-specific prompts.
    
    Args:
        intent_result: Dict with plugin_name and natural_language_request
        settings: Config dict
        
    Returns:
        str: AI response with JSON plan
    """
    plugin_name = intent_result.get('plugin_name')
    natural_language_request = intent_result.get('natural_language_request')
    
    # Map plugin to technology-specific prompt
    prompt_mapping = {
        'postgres': 'check_generation/postgres_check_prompt.adoc',
        'cassandra': 'check_generation/cassandra_check_prompt.adoc',
        'mongodb': 'check_generation/mongodb_check_prompt.adoc',
        'valkey': 'check_generation/valkey_check_prompt.adoc',
        'redis': 'check_generation/valkey_check_prompt.adoc',  # Alias
        'kafka': 'check_generation/kafka_check_prompt.adoc',
    }
    
    # Get technology-specific prompt or fall back to base
    prompt_file = prompt_mapping.get(
        plugin_name,
        'check_generation/base_check_prompt.adoc'  # Fallback for unknown plugins
    )
    
    print(f"  [DEBUG] Using prompt: {prompt_file}")
    
    # Render prompt with context
    from lib.templates import render_prompt
    prompt = render_prompt(prompt_file, {
        'plugin_name': plugin_name,
        'natural_language_request': natural_language_request
    })
    
    # Execute AI generation
    from lib.ai_client import execute_ai_prompt
    from lib.intent import execute_plan_from_ai
    
    response = execute_ai_prompt(prompt, settings)
    execute_plan_from_ai(response, settings)
