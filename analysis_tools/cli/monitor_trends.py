# analysis_tools/cli/monitor_trends.py

import yaml
import psycopg2
from cryptography.fernet import Fernet
import json
import argparse
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from deepdiff import DeepDiff
import re

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
    decrypted_bytes = fernet.decrypt(bytes(encrypted_data))
    try:
        return json.loads(decrypted_bytes)
    except (json.JSONDecodeError, TypeError):
        return decrypted_bytes.decode('utf-8')

def fetch_and_decrypt_runs(db_config, fernet, company_id, limit=2):
    """Fetches the latest runs for a company and decrypts them."""
    conn = None
    decrypted_runs = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        query = """
        SELECT run_timestamp, findings
        FROM health_check_runs
        WHERE company_id = %s
        ORDER BY run_timestamp DESC
        LIMIT %s;
        """
        cursor.execute(query, (company_id, limit))
        rows = cursor.fetchall()
        print(f"Found {len(rows)} health check runs for company_id {company_id}.")

        for row in rows:
            run_timestamp, enc_findings = row
            decrypted_run = {
                "run_timestamp": run_timestamp.isoformat(),
                "findings": decrypt_data(enc_findings, fernet)
            }
            decrypted_runs.append(decrypted_run)
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn: conn.close()
    return decrypted_runs

def format_path(path):
    """Cleans up the deepdiff path for readability."""
    # Remove root['...'] and extra quotes
    return re.sub(r"root\['(.*?)'\]", r'\1', path).replace("'", "")

def display_polished_summary(runs):
    """
    Compares the two most recent runs and prints a polished, readable summary.
    """
    if len(runs) < 2:
        print("\nInsufficient data for comparison. Need at least two runs.")
        return

    latest_run = runs[0]
    previous_run = runs[1]

    print("\n" + "="*80)
    print(f"🔍 Comparison Summary")
    print(f"  - Latest Run:   {latest_run['run_timestamp']}")
    print(f"  - Previous Run: {previous_run['run_timestamp']}")
    print("="*80)

    diff = DeepDiff(previous_run['findings'], latest_run['findings'], ignore_order=True, view='tree')

    if not diff:
        print("\n✅ No changes detected between the two most recent runs.")
        print("="*80)
        return

    changes_by_check = {}

    # Group all changes by the top-level check name
    for change_type, items in diff.items():
        for item in items:
            # Extract the top-level check name (e.g., 'cache_analysis')
            top_level_check = item.path(output_format='list')[0]
            if top_level_check not in changes_by_check:
                changes_by_check[top_level_check] = []
            
            # Format a readable summary of the change
            if change_type == 'values_changed':
                path = format_path(item.path())
                changes_by_check[top_level_check].append(
                    f"  🔄 Value changed at '{path}': '{item.t1}' -> '{item.t2}'"
                )
            elif change_type == 'iterable_item_added':
                path = format_path(item.path())
                changes_by_check[top_level_check].append(
                    f"  ➕ Item added to list at '{path}': {json.dumps(item.t2)}"
                )
            # Add other change types here as needed (item_removed, etc.)

    # Now, print the grouped and formatted summary
    print("\n--- Detected Changes by Check ---")
    for check, changes in sorted(changes_by_check.items()):
        print(f"\n  * In '{check}':")
        for change_summary in changes:
            print(f"    {change_summary}")

    print("\n" + "="*80)

def main():
    parser = argparse.ArgumentParser(description='Monitor and analyze health check trends.')
    parser.add_argument('--company-id', type=int, required=True, help='The ID of the company to analyze.')
    args = parser.parse_args()

    config = load_trends_config()
    if not config: return

    key_string = config.get('encryption_key')
    if not key_string:
        print("Error: 'encryption_key' not found in trends.yaml.")
        return
        
    fernet = Fernet(get_encryption_key(key_string))
    db_settings = config.get('database')
    
    runs = fetch_and_decrypt_runs(db_settings, fernet, args.company_id, limit=2)
    display_polished_summary(runs)

if __name__ == '__main__':
    main()
