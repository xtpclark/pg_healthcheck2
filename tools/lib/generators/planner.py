# lib/generators/planner.py
"""Comprehensive check planning."""
import json
from lib.ai_client import execute_ai_prompt
from lib.templates import render_prompt
from lib.validators import clean_ai_response
from lib.intent import recognize_intent_and_dispatch

def handle_plan_comprehensive_checks(entities, settings, user_query):
    """
    Handles multi-step comprehensive check planning workflow.
    
    Args:
        entities: Extracted entities from intent recognition
        settings: Config dict
        user_query: Original user query
    """
    print("\n💡 Formulating a comprehensive plan...")
    
    planner_prompt = render_prompt("planner_prompt.adoc", {
        "user_query": user_query
    })

    # Use high-reasoning model for strategic planning
    planning_model = settings.get('high_reasoning_model', settings.get('ai_model'))
    plan_response_raw = execute_ai_prompt(planner_prompt, settings, model_override=planning_model)

    if not plan_response_raw:
        return

    try:
        plan_json = json.loads(clean_ai_response(plan_response_raw))
        tasks = plan_json.get("plan", [])
        
        if not tasks:
            print("⚠️  AI returned an empty plan.")
            return

        print("\n🤖 I have formulated a plan to create the following solutions:")
        for i, task in enumerate(tasks, 1):
            print(f"   {i}. {task}")

        # Check for auto-execute setting
        interactive = settings.get('features', {}).get('interactive_mode', True)
        mode = 'ask' if interactive else 'all'

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

            # Build execution query
            tech_name = entities.get("technology_name", "").lower()
            if not tech_name and "postgres" in user_query.lower():
                tech_name = "postgres"
            
            execution_query = f"add a {tech_name} check for {task}"
            recognize_intent_and_dispatch(execution_query, settings)

        print("\n✅ Comprehensive plan executed successfully.")

    except Exception as e:
        print(f"❌ Failed to parse AI's execution plan. Error: {e}")
        print(f"Raw Response: {plan_response_raw}")
