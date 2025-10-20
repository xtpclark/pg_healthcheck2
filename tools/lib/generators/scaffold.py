# lib/generators/scaffold.py
"""Plugin scaffolding handler with template variable substitution."""
import json
from lib.ai_client import execute_ai_prompt
from lib.templates import render_prompt
from lib.operations.files import execute_operations
from lib.validators import clean_ai_response


def handle_scaffold_plugin(entities, settings):
    """
    Scaffolds a new plugin with proper variable substitution.
    
    Args:
        entities: Dict with 'technology' or 'technology_name'
        settings: Config dict
    """
    # Get technology name from entities
    tech_name = entities.get("technology_name") or entities.get("technology", "UnknownPlugin")
    
    print(f"\n💡 Scaffolding new plugin for {tech_name}...")
    
    # Prepare template variables for the prompt
    tech_lower = tech_name.lower().replace(' ', '_').replace('-', '_')
    tech_title = ''.join(word.capitalize() for word in tech_name.replace('_', ' ').replace('-', ' ').split())
    
    old_prompt_variables = {
        'technology': tech_name,
        'technology_lower': tech_lower,
        'Technology': tech_title
    }

    prompt_variables = {
        'technology_name': tech_name,                    # Match v4 prompt
        'technology_name_lowercase': tech_lower,          # Match v4 prompt
        'TechnologyNameCamelCase': tech_title            # Match v4 prompt
    }
    
    # Render the scaffold prompt
    scaffold_prompt = render_prompt("plugin_scaffold_prompt_v4.adoc", prompt_variables)
    
    # Call AI with generation model
    generation_model = settings.get('generation_model', settings.get('ai_model'))
    response = execute_ai_prompt(scaffold_prompt, settings, model_override=generation_model)
    
    if not response:
        print("❌ AI failed to generate scaffold plan.")
        return

    # ADD THIS: Strip markdown code fences before processing
    response = response.strip()
    if response.startswith('```json'):
        response = response[7:]  # Remove ```json
    elif response.startswith('```'):
        response = response[3:]  # Remove ```
    
    if response.endswith('```'):
        response = response[:-3]  # Remove closing ```
    
    response = response.strip()
    

    # Show debug output if enabled
    if settings.get('debug', False):
        print("\n=== RAW AI RESPONSE ===")
        print(response[:1000] + "..." if len(response) > 1000 else response)
        print("=== END RAW RESPONSE ===\n")
    
    try:
        # Clean and parse JSON response
        cleaned = clean_ai_response(response, "json")
        plan = json.loads(cleaned)
        
        # Validate structure
        if "operations" not in plan:
            print("⚠️  AI response missing 'operations' key")
            print(f"   Available keys: {list(plan.keys())}")
            return
        
        operations = plan.get("operations", [])
        if not operations:
            print("⚠️  AI did not provide any file operations.")
            return
        
        # === CRITICAL: Replace template variables in ALL operations ===
        print(f"📦 Processing {len(operations)} operations...")
        
        substituted_plan = replace_template_variables(plan, {
            '{technology_lower}': tech_lower,
            '{Technology}': tech_title,
            '{technology}': tech_name,
            '{{technology_lower}}': tech_lower,  # Handle double-brace variants
            '{{Technology}}': tech_title,
            '{{technology}}': tech_name
        })
        
        # Execute operations with substituted values
        success = execute_operations(substituted_plan['operations'], settings)
        
        if success:
            print(f"\n✅ Successfully scaffolded {tech_name} plugin!")
            
            # Show post message with variable substitution
            if 'post_message' in substituted_plan:
                print(f"\n{substituted_plan['post_message']}")
            
            # Show integration instruction with variable substitution
            if 'integration_step' in substituted_plan:
                integration = substituted_plan['integration_step']
                print(f"\n📝 Next Step: {integration.get('instruction', '')}")
                
                if 'code_snippet_to_add' in integration:
                    print(f"\n{integration['code_snippet_to_add']}")
        else:
            print(f"❌ Some operations failed during scaffolding")
    
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse AI response as JSON")
        print(f"   Error: {e}")
        print(f"   Position: line {e.lineno}, column {e.colno}")
        
        # Show context around error
        if e.doc:
            lines = e.doc.split('\n')
            if e.lineno <= len(lines):
                start = max(0, e.lineno - 3)
                end = min(len(lines), e.lineno + 2)
                print("\n   Context:")
                for i in range(start, end):
                    marker = " >>> " if i == e.lineno - 1 else "     "
                    print(f"{marker}{i+1}: {lines[i][:80]}")
    
    except Exception as e:
        print(f"❌ Failed to scaffold plugin: {e}")
        if settings.get('debug', False):
            import traceback
            traceback.print_exc()


def replace_template_variables(obj, replacements):
    """
    Recursively replace template variables in all strings within a data structure.
    
    Args:
        obj: Data structure (dict, list, str, etc.)
        replacements: Dict mapping template strings to replacement values
    
    Returns:
        Modified data structure with all replacements applied
    """
    if isinstance(obj, dict):
        return {k: replace_template_variables(v, replacements) for k, v in obj.items()}
    
    elif isinstance(obj, list):
        return [replace_template_variables(item, replacements) for item in obj]
    
    elif isinstance(obj, str):
        result = obj
        for template, replacement in replacements.items():
            result = result.replace(template, replacement)
        return result
    
    else:
        # Return unchanged for other types (int, bool, None, etc.)
        return obj
