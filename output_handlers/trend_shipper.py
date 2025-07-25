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

def ship_to_database(db_config, encrypted_data):
    """Connects to PostgreSQL and inserts the encrypted health check data."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_config['host'], port=db_config.get('port', 5432),
            dbname=db_config['dbname'], user=db_config['user'],
            password=db_config['password'], sslmode=db_config.get('sslmode', 'prefer')
        )
        cursor = conn.cursor()
        print("Log: Successfully connected to PostgreSQL.")

        insert_query = """
        INSERT INTO health_check_runs 
        (company_id, db_technology, target_host, target_port, target_db_name, findings)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        
        cursor.execute(insert_query, (
            encrypted_data['company_id'], encrypted_data['db_technology'],
            encrypted_data['target_host'], encrypted_data['target_port'],
            encrypted_data['target_db_name'], encrypted_data['findings']
        ))

        conn.commit()
        print("Log: Successfully inserted encrypted health check run into the database.")
        
    except psycopg2.Error as e:
        print(f"Error: Failed to ship data to PostgreSQL. {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def ship_to_api(api_config, encrypted_data):
    """Sends encrypted health check data to the specified API endpoint."""
    try:
        headers = {
            'Authorization': f"Bearer {api_config['api_key']}",
            'Content-Type': 'application/json'
        }

        import base64
        json_payload = {k: base64.b64encode(v).decode('utf-8') if isinstance(v, bytes) else v for k, v in encrypted_data.items()}

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
    
    # --- REAL FLOW: Use the provided target_info dictionary ---
    encrypted_payload = {
        'company_id': config.get('company_id'),
        'db_technology': target_info.get('db_type', 'unknown'),
        'target_host': encrypt_data(target_info.get('host', 'unknown'), fernet),
        'target_port': encrypt_data(target_info.get('port', 0), fernet),
        'target_db_name': encrypt_data(target_info.get('database', 'unknown'), fernet),
        'findings': encrypt_data(structured_findings, fernet)
    }
    # --- The placeholder logic is now removed. ---

    destination = config.get('destination')

    if destination == "postgresql":
        ship_to_database(config.get('database'), encrypted_payload)
    elif destination == "api":
        ship_to_api(config.get('api'), encrypted_payload)
    else:
        print(f"Error: Unknown trend storage destination '{destination}'.")
        
    print("--- Trend Shipper Module Finished ---")
