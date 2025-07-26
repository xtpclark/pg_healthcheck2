import yaml
import psycopg2
import requests
import json
from decimal import Decimal
from datetime import datetime, timedelta

# This encoder is still needed to handle special data types for JSON serialization.
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
    """Connects to PostgreSQL and inserts the raw health check data.
    It assumes a database trigger will handle the encryption.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['host'], port=db_config.get('port', 5432),
            dbname=db_config['dbname'], user=db_config['user'],
            password=db_config['password'], sslmode=db_config.get('sslmode', 'prefer')
        )
        cursor = conn.cursor()
        print("Log: Successfully connected to PostgreSQL for trend shipping.")

        company_name = target_info.get('company_name', 'Default Company')
        db_technology = target_info.get('db_type', 'unknown')
        host = target_info.get('host', 'unknown')
        port = target_info.get('port', 0)
        database = target_info.get('database', 'unknown')

        # The 'raw_findings' column will be used by a server-side trigger to encrypt the data.
        # This insert sends unencrypted metadata and the raw JSON findings.
        insert_query = """
        INSERT INTO health_check_runs (company_name, db_technology, target_host, target_port, target_db_name, raw_findings)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (company_name, db_technology, host, port, database, findings_json))

        conn.commit()
        print("Log: Successfully shipped raw findings to the database.")

    except psycopg2.Error as e:
        print(f"Error: Failed to ship data to PostgreSQL. {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def ship_to_api(api_config, target_info, findings):
    """Sends raw, unencrypted health check data to the specified API endpoint."""
    try:
        headers = {'Content-Type': 'application/json'}
        
        # The payload now contains the unencrypted target info and findings.
        full_payload = {
            'target_info': target_info,
            'findings': findings
        }
        
        response = requests.post(
            api_config['endpoint_url'],
            headers=headers,
            data=json.dumps(full_payload, cls=CustomJsonEncoder),
            timeout=15
        )
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        print(f"Log: Successfully sent raw findings to API. Status: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to ship data to API. {e}")

def run(structured_findings, target_info):
    """
    Main entry point for the trend shipper. Sends unencrypted data.
    """
    print("--- Trend Shipper Module Started ---")
    config = load_config()
    if not config:
        print("--- Trend Shipper Module Finished (No Config) ---")
        return

    destination = config.get('destination')

    if destination == "postgresql":
        # Convert findings to a JSON string for the database call
        findings_as_json = json.dumps(structured_findings, cls=CustomJsonEncoder)
        ship_to_database(config.get('database'), target_info, findings_as_json)
    elif destination == "api":
        # The API can handle the raw dictionary
        ship_to_api(config.get('api'), target_info, structured_findings)
    else:
        print(f"Error: Unknown trend storage destination '{destination}'.")
        
    print("--- Trend Shipper Module Finished ---")
