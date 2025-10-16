# lib/templates.py
"""Jinja2 template rendering for prompts."""
import jinja2
from pathlib import Path

def render_prompt(template_name, context):
    """
    Loads and renders a Jinja2 prompt template.
    
    Args:
        template_name: Name of template file (e.g., "intent_recognizer_prompt.adoc")
        context: Dict of variables to pass to template
        
    Returns:
        str: Rendered prompt
        
    Raises:
        SystemExit: If template not found
    """
    template_dir = Path(__file__).parent.parent / "templates"
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(template_dir)),
        trim_blocks=True,
        lstrip_blocks=True
    )
    
    try:
        template = env.get_template(template_name)
        return template.render(context)
    except jinja2.exceptions.TemplateNotFound:
        print(f"❌ Error: Prompt template not found at {template_dir / template_name}")
        exit(1)
