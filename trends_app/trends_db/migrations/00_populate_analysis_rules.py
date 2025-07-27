import sys
import json
from pathlib import Path
import psycopg2
import yaml

# --- Setup Project Path ---
# This allows the script to import modules from your project root (e.g., plugins)
# when run from the root directory.
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# --- Import the rules from the main application ---
try:
    from plugins.postgres.rules.analysis_rules import METRIC_ANALYSIS_CONFIG
except ImportError as e:
    print(f"Error: Could not import analysis rules. Make sure you run this script from the project root.")
    print(f"Details: {e}")
    sys.exit(1)

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
    Connects to the database and inserts the imported analysis rules.
    """
    db_config = load_db_config()
    if not db_config:
        sys.exit(1)

    # --- Define the Rule Set to be Inserted ---
    rule_set_name = "Default PostgreSQL Rules"
    technology = "postgres"
    rules_json = json.dumps(METRIC_ANALYSIS_CONFIG) # Convert the Python dict to a JSON string

    conn = None
    try:
        print("Connecting to the health_trends database...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Check if this rule set already exists to prevent duplicates
        cursor.execute("SELECT id FROM analysis_rules WHERE rule_set_name = %s;", (rule_set_name,))
        if cursor.fetchone():
            print(f"'{rule_set_name}' already exists in the database. Skipping insertion.")
            return

        # Insert the new rule set
        print(f"Inserting '{rule_set_name}' into the analysis_rules table...")
        insert_query = """
            INSERT INTO analysis_rules (rule_set_name, technology, rules_json)
            VALUES (%s, %s, %s);
        """
        cursor.execute(insert_query, (rule_set_name, technology, rules_json))
        
        conn.commit()
        print("✅ Successfully populated analysis_rules table.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
