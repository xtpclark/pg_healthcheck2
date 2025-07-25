import yaml
import psycopg2
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
    # A key from config must be 32 bytes and base64 encoded.
    # For simplicity, we'll derive a key, but production systems should use a secure key management service.
    import base64
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    
    salt = b'some-static-salt' # In production, this should also be unique and stored.
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    return base64.urlsafe_b64encode(kdf.derive(key_string.encode()))

def encrypt_data(data, fernet):
    """Encrypts data using the provided Fernet instance."""
    # Ensure data is in bytes format for encryption
    if isinstance(data, dict) or isinstance(data, list):
        data_bytes = json.dumps(data, cls=CustomJsonEncoder).encode('utf-8')
    elif isinstance(data, (int, float)):
        data_bytes = str(data).encode('utf-8')
    elif isinstance(data, str):
        data_bytes = data.encode('utf-8')
    else:
        data_bytes = data # Assume it's already bytes

    return fernet.encrypt(data_bytes)

def ship_to_database(db_config, encrypted_data):
    """Connects to PostgreSQL and inserts the encrypted health check data."""
    conn = None
    try:
        # Establish connection
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config.get('port', 5432),
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            sslmode=db_config.get('sslmode', 'prefer') # 'require' is better for production
        )
        cursor = conn.cursor()
        print("Log: Successfully connected to PostgreSQL.")

        # Prepare the INSERT statement
        insert_query = """
        INSERT INTO health_check_runs 
        (company_id, db_technology, target_host, target_port, target_db_name, findings)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        
        # Execute the query
        cursor.execute(insert_query, (
            encrypted_data['company_id'],
            encrypted_data['db_technology'],
            encrypted_data['target_host'],
            encrypted_data['target_port'],
            encrypted_data['target_db_name'],
            encrypted_data['findings']
        ))

        conn.commit()
        print("Log: Successfully inserted encrypted health check run into the database.")
        
    except psycopg2.Error as e:
        print(f"Error: Failed to ship data to PostgreSQL. {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def ship_to_api(config, data):
    """Placeholder for sending data to an API endpoint."""
    print(f"Log: Shipping data to API endpoint at {config['endpoint_url']} (Not Implemented).")
    return True

def run(structured_findings):
    """
    Main entry point for the trend shipper.
    """
    print("--- Trend Shipper Module Started ---")
    config = load_config()
    
    if not config:
        print("--- Trend Shipper Module Finished (No Config) ---")
        return

    # In production, the key would be fetched securely
    key_string = config.get('encryption_key')
    if not key_string:
        print("Error: 'encryption_key' not found in trends.yaml.")
        return
        
    fernet = Fernet(get_encryption_key(key_string))
    
    # Prepare data for encryption and insertion
    # Note: In a real flow, these values would come from the main config/connector
    encrypted_payload = {
        'company_id': config.get('company_id', 1), # Placeholder
        'db_technology': structured_findings.get('db_type', 'postgresql'), # Placeholder
        'target_host': encrypt_data(config['database']['host'], fernet),
        'target_port': encrypt_data(config['database']['port'], fernet),
        'target_db_name': encrypt_data(config['database']['dbname'], fernet),
        'findings': encrypt_data(structured_findings, fernet)
    }

    destination = config.get('destination')

    if destination == "postgresql":
        ship_to_database(config.get('database'), encrypted_payload)
    elif destination == "api":
        ship_to_api(config.get('api'), encrypted_payload)
    else:
        print(f"Error: Unknown trend storage destination '{destination}'.")
        
    print("--- Trend Shipper Module Finished ---")
