import sys
from pathlib import Path
import psycopg2
import yaml
import os
import argparse

# --- Setup Project Path ---
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

def load_db_config():
    """Loads database configuration from the main trends.yaml file."""
    config_path = project_root / 'config' / 'trends.yaml'
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            return config.get('database')
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
        return None
    except Exception as e:
        print(f"Error reading configuration: {e}")
        return None

def main():
    """
    Connects to the database and inserts Jinja2 prompt templates from a
    specified plugin directory.
    """
    parser = argparse.ArgumentParser(description='Populate the database with prompt templates from a plugin.')
    parser.add_argument('--plugin', required=True, help='The name of the plugin directory (e.g., postgres, cassandra).')
    args = parser.parse_args()

    db_config = load_db_config()
    if not db_config:
        sys.exit(1)

    # --- Dynamically Discover Templates ---
    technology = args.plugin
    templates_path = project_root / 'plugins' / technology / 'templates' / 'prompts'

    if not templates_path.is_dir():
        print(f"Error: Directory not found for plugin '{technology}': {templates_path}")
        sys.exit(1)

    templates_to_load = []
    for entry in os.scandir(templates_path):
        if entry.is_file() and entry.name.endswith('.j2'):
            # Create a user-friendly name from the filename
            # e.g., 'executive_summary_template.j2' -> 'Executive Summary'
            name = Path(entry.name).stem.replace('_template', '').replace('_', ' ').title()
            templates_to_load.append({
                'file_path': Path(entry.path),
                'name': name,
                'tech': technology
            })
    
    if not templates_to_load:
        print(f"No .j2 templates found in {templates_path}. Nothing to do.")
        return

    conn = None
    try:
        print("Connecting to the health_trends database...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        for template_info in templates_to_load:
            try:
                with open(template_info['file_path'], 'r') as f:
                    content = f.read()
            except IOError as e:
                print(f"Warning: Could not read file {template_info['file_path']}. Error: {e}. Skipping.")
                continue

            # Check if this template already exists
            cursor.execute("SELECT id FROM prompt_templates WHERE template_name = %s;", (template_info['name'],))
            if cursor.fetchone():
                print(f"Template '{template_info['name']}' already exists. Skipping.")
                continue

            # Insert the new template
            print(f"Inserting template '{template_info['name']}'...")
            insert_query = """
                INSERT INTO prompt_templates (template_name, technology, template_content)
                VALUES (%s, %s, %s);
            """
            cursor.execute(insert_query, (template_info['name'], template_info['tech'], content))
        
        conn.commit()
        print("✅ Successfully populated prompt_templates table.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
