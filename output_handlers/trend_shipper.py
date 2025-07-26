import yaml
import psycopg2
import requests
import json
from decimal import Decimal
from datetime import datetime, timedelta

class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        if isinstance(obj, datetime): return obj.isoformat()
        if isinstance(obj, timedelta): return obj.total_seconds()
        return json.JSONEncoder.default(self, obj)

def load_config(config_path='config/trends.yaml'):
    """Loads the trend shipper configuration."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print("Log: trends.yaml not found. Skipping trend analysis.")
        return None
    except Exception as e:
        print(f"Error loading trends.yaml: {e}")
        return None

def ship_to_database(db_config, target_info, findings_json):
    """
    Connects to PostgreSQL, gets or creates the company ID using the
    database function, and inserts the raw health check data.
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("Log: Successfully connected to PostgreSQL for trend shipping.")

        # Get the company name from the main config dictionary
        company_name = target_info.get('company_name', 'Default Company')
        
        # --- FIX: Call the database function to get the company_id ---
        cursor.execute("SELECT get_or_create_company(%s);", (company_name,))
        company_id = cursor.fetchone()[0]
        print(f"Log: Using company_id '{company_id}' for company '{company_name}'.")

        db_technology = target_info.get('db_type', 'unknown')
        host = target_info.get('host', 'unknown')
        port = target_info.get('port', 0)
        database = target_info.get('database', 'unknown')

        # --- FIX: The INSERT statement now uses company_id ---
        insert_query = """
        INSERT INTO health_check_runs (company_id, db_technology, target_host, target_port, target_db_name, findings)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb);
        """
        cursor.execute(insert_query, (company_id, db_technology, host, port, database, findings_json))

        conn.commit()
        print("Log: Successfully shipped raw findings to the database.")

    except psycopg2.Error as e:
        print(f"Error: Failed to ship data to PostgreSQL. {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def ship_to_api(api_config, target_info, findings):
    """Sends raw health check data to the specified API endpoint."""
    try:
        headers = {'Content-Type': 'application/json'}
        full_payload = {'target_info': target_info, 'findings': findings}
        response = requests.post(api_config['endpoint_url'], headers=headers, data=json.dumps(full_payload, cls=CustomJsonEncoder), timeout=15)
        response.raise_for_status()
        print(f"Log: Successfully sent raw findings to API. Status: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to ship data to API. {e}")

def run(structured_findings, target_info):
    """Main entry point for the trend shipper."""
    print("--- Trend Shipper Module Started ---")
    config = load_config()
    if not config:
        print("--- Trend Shipper Module Finished (No Config) ---")
        return

    destination = config.get('destination')

    if destination == "postgresql":
        findings_as_json = json.dumps(structured_findings, cls=CustomJsonEncoder)
        ship_to_database(config.get('database'), target_info, findings_as_json)
    elif destination == "api":
        ship_to_api(config.get('api'), target_info, structured_findings)
    else:
        print(f"Error: Unknown trend storage destination '{destination}'.")
        
    print("--- Trend Shipper Module Finished ---")
