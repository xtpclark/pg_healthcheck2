import yaml
import psycopg2
import requests 
from cryptography.fernet import Fernet
import json
from decimal import Decimal
from datetime import datetime, timedelta

# This encoder is needed to handle data types in the findings dict before encryption.
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

def get_encryption_key(key_string):
    """Generates a valid Fernet key from the string in the config."""
    import base64
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    
    salt = b'some-static-salt'
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    return base64.urlsafe_b64encode(kdf.derive(key_string.encode()))

def encrypt_data(data, fernet):
    """Encrypts data using the provided Fernet instance."""
    if isinstance(data, dict) or isinstance(data, list):
        data_bytes = json.dumps(data, cls=CustomJsonEncoder).encode('utf-8')
    elif isinstance(data, (int, float)):
        data_bytes = str(data).encode('utf-8')
    elif isinstance(data, str):
        data_bytes = data.encode('utf-8')
    else:
        data_bytes = data

    return fernet.encrypt(data_bytes)

def ship_to_database(db_config, target_info, encrypted_payload):
    """
    Connects to PostgreSQL, gets or creates the company ID, and inserts the data.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['host'], port=db_config.get('port', 5432),
            dbname=db_config['dbname'], user=db_config['user'],
            password=db_config['password'], sslmode=db_config.get('sslmode', 'prefer')
        )
        cursor = conn.cursor()
        print("Log: Successfully connected to PostgreSQL.")

        # Get or create the company ID using the database function
        company_name = target_info.get('company_name', 'Default Company')
        cursor.execute("SELECT get_or_create_company(%s);", (company_name,))
        company_id = cursor.fetchone()[0]
        print(f"Log: Using company_id '{company_id}' for company '{company_name}'.")

        insert_query = """
        INSERT INTO health_check_runs 
        (company_id, db_technology, target_host, target_port, target_db_name, findings)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        
        cursor.execute(insert_query, (
            company_id,
            encrypted_payload['db_technology'],
            encrypted_payload['target_host'],
            encrypted_payload['target_port'],
            encrypted_payload['target_db_name'],
            encrypted_payload['findings']
        ))

        conn.commit()
        print("Log: Successfully inserted encrypted health check run into the database.")
        
    except psycopg2.Error as e:
        print(f"Error: Failed to ship data to PostgreSQL. {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def ship_to_api(api_config, target_info, encrypted_data):
    """
    Sends encrypted health check data and company info to the specified API endpoint.
    """
    try:
        headers = {
            'Authorization': f"Bearer {api_config['api_key']}",
            'Content-Type': 'application/json'
        }

        import base64
        # Add company_name to the payload for the API
        full_payload = {
            'company_name': target_info.get('company_name'),
            **encrypted_data
        }

        json_payload = {k: base64.b64encode(v).decode('utf-8') if isinstance(v, bytes) else v for k, v in full_payload.items()}

        response = requests.post(api_config['endpoint_url'], headers=headers, json=json_payload, timeout=15)
        response.raise_for_status()
        
        print(f"Log: Successfully sent health check data to API. Status: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to ship data to API. {e}")
    except Exception as e:
        print(f"An unexpected error occurred during API shipping: {e}")

def run(structured_findings, target_info):
    """
    Main entry point for the trend shipper.
    Accepts findings and the target database connection info.
    """
    print("--- Trend Shipper Module Started ---")
    config = load_config()
    
    if not config:
        print("--- Trend Shipper Module Finished (No Config) ---")
        return

    key_string = config.get('encryption_key')
    if not key_string:
        print("Error: 'encryption_key' not found in trends.yaml.")
        return
        
    fernet = Fernet(get_encryption_key(key_string))
    
    # Create the encrypted payload
    encrypted_payload = {
        'db_technology': target_info.get('db_type', 'unknown'),
        'target_host': encrypt_data(target_info.get('host', 'unknown'), fernet),
        'target_port': encrypt_data(target_info.get('port', 0), fernet),
        'target_db_name': encrypt_data(target_info.get('database', 'unknown'), fernet),
        'findings': encrypt_data(structured_findings, fernet)
    }

    destination = config.get('destination')

    if destination == "postgresql":
        # Pass the full target_info for company name lookup
        ship_to_database(config.get('database'), target_info, encrypted_payload)
    elif destination == "api":
        # Pass the full target_info to the API as well
        ship_to_api(config.get('api'), target_info, encrypted_payload)
    else:
        print(f"Error: Unknown trend storage destination '{destination}'.")
        
    print("--- Trend Shipper Module Finished ---")
