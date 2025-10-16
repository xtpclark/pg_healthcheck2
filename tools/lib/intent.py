# lib/intent.py
"""Intent recognition and routing."""
import json
from lib.ai_client import execute_ai_prompt
from lib.templates import render_prompt
from lib.validators import clean_ai_response

def recognize_intent_and_dispatch(user_query, settings):
    """
    Recognizes user intent and dispatches to appropriate handler.
    
    Args:
        user_query: User's natural language request
        settings: Config dict
        
    Returns:
        None (handlers print their own output)
    """
    print(f"\n🤔 Understanding your request: \"{user_query}\"")
    
    intent_prompt = render_prompt("intent_recognizer_prompt.adoc", {
        "user_query": user_query
    })
    
    # Use fast model for classification
    fast_model = settings.get('fast_model', settings.get('ai_model'))
    intent_response_raw = execute_ai_prompt(intent_prompt, settings, model_override=fast_model)

    if not intent_response_raw:
        return

    try:
        intent_json = json.loads(clean_ai_response(intent_response_raw))
        intent = intent_json.get("intent")
        entities = intent_json.get("entities")
        
        print(f"  - Intent recognized: '{intent}'")
        
        # Import handlers here to avoid circular imports
        from lib.generators.check import handle_generate_check
        from lib.generators.scaffold import handle_scaffold_plugin
        from lib.generators.planner import handle_plan_comprehensive_checks
        
        handler_map = {
            "generate-check": handle_generate_check,
            "generate-rule": lambda e, s: generic_handler("Generating new JSON rule", "generate_rule_prompt.adoc", e, s),
            "scaffold-plugin": handle_scaffold_plugin,
            "add-report": lambda e, s: generic_handler("Adding new report", "add_report_prompt.adoc", e, s),
            "add-check": lambda e, s: generic_handler("Adding new boilerplate check", "add_check_prompt.adoc", e, s),
            "plan_comprehensive_checks": handle_plan_comprehensive_checks
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
    """Generic handler for simple single-prompt tasks."""
    print(f"\n💡 {task_name}...")
    
    from lib.templates import render_prompt
    from lib.operations.files import execute_operations
    from lib.integrators.report import handle_code_integration
    
    action_prompt = render_prompt(prompt_template, entities)
    
    generation_model = settings.get('generation_model', settings.get('ai_model'))
    plan_response_raw = execute_ai_prompt(action_prompt, settings, model_override=generation_model)
    
    execute_plan_from_ai(plan_response_raw, settings)

def execute_plan_from_ai(plan_response_raw, settings):
    """Parses and executes an AI-generated plan."""
    if not plan_response_raw:
        print("❌ AI failed to generate an execution plan.")
        return

    try:
        from lib.operations.files import execute_operations
        from lib.integrators.report import handle_code_integration
        
        plan_json = json.loads(clean_ai_response(plan_response_raw, "json"))
        operations = plan_json.get("operations")
        integration_step = plan_json.get("integration_step")
        
        auto_integrate = settings.get('operations', {}).get('auto_integrate', True)
        
        if execute_operations(operations, settings):
            handle_code_integration(integration_step, settings, auto_integrate)
    
    except Exception as e:
        print(f"❌ Failed to process AI's execution plan. Error: {e}")
        print(f"Raw Response: {plan_response_raw}")
