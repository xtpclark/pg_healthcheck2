# analysis_tools/cli/monitor_trends.py

import yaml
import psycopg2
from cryptography.fernet import Fernet
import json
import argparse
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

def load_trends_config(config_path='config/trends.yaml'):
    """Loads the trend shipper configuration."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print("Error: trends.yaml not found. Cannot connect to the database.")
        return None
    except Exception as e:
        print(f"Error loading trends.yaml: {e}")
        return None

def get_encryption_key(key_string):
    """Generates a valid Fernet key from the string in the config."""
    # This function must be identical to the one in trend_shipper.py
    salt = b'some-static-salt'
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    return base64.urlsafe_b64encode(kdf.derive(key_string.encode()))

def decrypt_data(encrypted_data, fernet):
    """Decrypts data using the provided Fernet instance."""
    if not encrypted_data:
        return None
    decrypted_bytes = fernet.decrypt(encrypted_data)
    # Attempt to load as JSON, fall back to plain text if it fails
    try:
        return json.loads(decrypted_bytes)
    except (json.JSONDecodeError, TypeError):
        return decrypted_bytes.decode('utf-8')

def fetch_and_decrypt_runs(db_config, fernet, company_id, limit=5):
    """Fetches the latest runs for a company and decrypts them."""
    conn = None
    decrypted_runs = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        query = """
        SELECT run_timestamp, target_host, target_port, target_db_name, findings
        FROM health_check_runs
        WHERE company_id = %s
        ORDER BY run_timestamp DESC
        LIMIT %s;
        """
        cursor.execute(query, (company_id, limit))
        
        rows = cursor.fetchall()
        print(f"Found {len(rows)} health check runs for company_id {company_id}.")

        for row in rows:
            run_timestamp, enc_host, enc_port, enc_dbname, enc_findings = row
            # --- FIX: Explicitly cast the database output to bytes ---
            decrypted_run = {
                "run_timestamp": run_timestamp.isoformat(),
                "target_host": decrypt_data(bytes(enc_host), fernet),
                "target_port": decrypt_data(bytes(enc_port), fernet),
                "target_db_name": decrypt_data(bytes(enc_dbname), fernet),
                "findings": decrypt_data(bytes(enc_findings), fernet)
            }
            decrypted_runs.append(decrypted_run)

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
    
    return decrypted_runs

def display_runs(runs):
    """Simple function to print the decrypted runs."""
    if not runs:
        print("No data to display.")
        return

    for i, run in enumerate(runs):
        print("\n" + "="*50)
        print(f"--- Run {i+1} at {run['run_timestamp']} ---")
        print(f"Target: {run['target_host']}:{run['target_port']} ({run['target_db_name']})")
        print("-"*50)
        # Pretty print the findings JSON
        print(json.dumps(run['findings'], indent=2))
        print("="*50)

def main():
    parser = argparse.ArgumentParser(description='Monitor and analyze health check trends.')
    parser.add_argument('--company-id', type=int, required=True, help='The ID of the company to analyze.')
    parser.add_argument('--limit', type=int, default=2, help='Number of recent runs to fetch and compare.')
    args = parser.parse_args()

    config = load_trends_config()
    if not config:
        return

    key_string = config.get('encryption_key')
    if not key_string:
        print("Error: 'encryption_key' not found in trends.yaml.")
        return
        
    fernet = Fernet(get_encryption_key(key_string))
    
    db_settings = config.get('database')
    
    runs = fetch_and_decrypt_runs(db_settings, fernet, args.company_id, args.limit)
    display_runs(runs)

if __name__ == '__main__':
    main()
