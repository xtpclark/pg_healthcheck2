# lib/generators/scaffold.py
"""Plugin scaffolding handler."""
from lib.intent import generic_handler

def handle_scaffold_plugin(entities, settings):
    """Scaffolds a new plugin."""
    tech_name = entities.get("technology_name", "UnknownPlugin")
    entities['technology_name_lowercase'] = tech_name.lower().replace(' ', '').replace('-', '')
    entities['TechnologyNameCamelCase'] = tech_name.replace(' ', '').replace('-', '').title()
    
    generic_handler("Scaffolding new plugin", "plugin_scaffold_prompt.adoc", entities, settings)
