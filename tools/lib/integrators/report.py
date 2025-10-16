# lib/integrators/report.py
"""Report file integration module."""
import os
from pathlib import Path
from lib.validators import clean_ai_response
from lib.ai_client import execute_ai_prompt
from lib.templates import render_prompt

def handle_code_integration(integration_step, settings, auto_integrate=True):
    """
    Performs Read-Modify-Write operation for report integration.
    
    Args:
        integration_step: Integration step dict from AI
        settings: Config dict
        auto_integrate: If True, skip confirmation prompt
        
    Returns:
        bool: True if integration succeeded
    """
    if not integration_step:
        return False
    
    target_file_hint = integration_step.get("target_file_hint")
    if not target_file_hint:
        print("⚠️  Integration step is missing 'target_file_hint'. Skipping.")
        return False
    
    instruction = integration_step.get("instruction")
    code_to_add = integration_step.get("code_snippet_to_add")
    
    if not all([instruction, code_to_add]):
        print("⚠️  Integration step is incomplete. Skipping.")
        return False

    print("\n" + "="*50)
    print(f"💡 The AI has suggested an integration step for {target_file_hint}")
    
    # Check config for auto-integrate setting
    if not auto_integrate:
        choice = input("Shall I attempt to apply this change automatically? (Y/n) > ").lower().strip()
        if choice == 'n':
            return False

    # Save current directory and change to project root
    original_dir = Path.cwd()
    # report.py is at tools/lib/integrators/report.py, go up 3 levels to tools/, then 1 more to project root
    project_root = Path(__file__).parent.parent.parent.parent
    os.chdir(project_root)
    
    try:
        # Now target_file will be resolved relative to project root
        target_file = Path(target_file_hint)
        print(f"  [DEBUG] Project root: {project_root}")
        print(f"  [DEBUG] Target file: {target_file.resolve()}")
        
        original_code = ""
        
        # Read existing file or create stub
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

        # Ask AI to modify code
        modifier_prompt = render_prompt("code_modifier_prompt.adoc", {
            "original_code": original_code,
            "modification_instruction": f"{instruction}\n\nHere is the dictionary to add:\n{code_to_add}"
        })
        
        print("  - Asking AI to perform the code modification...")
        
        # Use high-reasoning model for modification
        modification_model = settings.get('high_reasoning_model', settings.get('ai_model'))
        modified_code_raw = execute_ai_prompt(modifier_prompt, settings, model_override=modification_model)
        
        if not modified_code_raw:
            raise ValueError("AI did not return any modified code.")
        
        modified_code = clean_ai_response(modified_code_raw, "python")
        target_file.write_text(modified_code, encoding='utf-8')
        
        print("✅ Code integration successful!")
        return True

    except Exception as e:
        print(f"❌ An error occurred during automatic code integration: {e}")
        return False
    
    finally:
        # Always restore original directory
        os.chdir(original_dir)
