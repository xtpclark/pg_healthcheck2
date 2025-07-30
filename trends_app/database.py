import psycopg2
import psycopg2.extras
import json
import boto3
from cryptography.fernet import Fernet
from flask import current_app
from .models import User
from .utils import load_trends_config

def check_db_connection():
    """Checks if a connection can be made to the database."""
    config = load_trends_config()
    if not config or 'database' not in config:
        return False, "trends.yaml not found or is missing 'database' section."
    
    db_config = config['database']
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        return True, "Successfully connected to the database."
    except psycopg2.Error as e:
        return False, f"Database connection failed: {str(e).splitlines()[0]}"
    finally:
        if conn:
            conn.close()

def load_user(db_config, user_id):
    """Loads a user and their associated companies and privileges."""
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, is_admin, password_change_required FROM users WHERE id = %s;", (user_id,))
        user_data = cursor.fetchone()
        if user_data:
            user_id, username, is_admin, password_change_required = user_data
            
            cursor.execute("SELECT c.id, c.company_name FROM user_company_access uca JOIN companies c ON uca.company_id = c.id WHERE uca.user_id = %s;", (user_id,))
            accessible_companies = [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]

            # This query now correctly joins usrgrp, grppriv, and priv to get all inherited privileges
            # and combines them with any directly assigned privileges.
            privileges_query = """
                SELECT p.priv_name FROM usrpriv up JOIN priv p ON up.usrpriv_priv_id = p.priv_id 
                WHERE up.usrpriv_username = %(username)s
                UNION
                SELECT p.priv_name FROM usrgrp ug
                JOIN grppriv gp ON ug.usrgrp_grp_id = gp.grppriv_grp_id
                JOIN priv p ON gp.grppriv_priv_id = p.priv_id
                WHERE ug.usrgrp_username = %(username)s;
            """
            cursor.execute(privileges_query, {'username': username})
            privileges = {row[0] for row in cursor.fetchall()}

            return User(user_id, username, is_admin, password_change_required, accessible_companies, privileges)
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error loading user: {e}")
    finally:
        if conn: conn.close()
    return None

def decrypt_kms_data_key(encrypted_key_blob, config):
    """Uses AWS KMS to decrypt a data key."""
    kms_client = boto3.client('kms')
    kms_key_arn = config.get('encryption', {}).get('aws_kms_key_arn')
    
    response = kms_client.decrypt(
        CiphertextBlob=encrypted_key_blob,
        KeyId=kms_key_arn
    )
    return response['Plaintext']

def get_unique_targets(db_config, accessible_company_ids):
    """Fetches unique targets accessible by the user to populate filters."""
    if not accessible_company_ids:
        return []
    
    conn = None
    targets = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        query = """
            SELECT DISTINCT c.company_name, hcr.target_host, hcr.target_port, hcr.target_db_name
            FROM health_check_runs hcr
            JOIN companies c ON hcr.company_id = c.id
            WHERE hcr.company_id = ANY(%s)
            ORDER BY c.company_name, hcr.target_host, hcr.target_port, hcr.target_db_name;
        """
        cursor.execute(query, (accessible_company_ids,))
        for row in cursor.fetchall():
            targets.append(f"{row[0]}:{row[1]}:{row[2]}:{row[3]}")
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching unique targets: {e}")
    finally:
        if conn: conn.close()
    return targets

def save_user_preference(db_config, username, pref_name, pref_value):
    """Saves a user preference by calling the setuserpreference database function."""
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT setuserpreference(%s, %s, %s);",
            (username, pref_name, pref_value)
        )
        conn.commit()
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error saving user preference via function: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()


def fetch_template_asset(db_settings, asset_name):
    """Fetches a single template asset's raw data from the database."""
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT asset_data FROM template_assets WHERE asset_name = %s;",
            (asset_name,)
        )
        result = cursor.fetchone()
        # Returns the binary data (bytea) if found, otherwise None
        return result[0] if result else None
    except psycopg2.Error as e:
        print(f"Database error fetching template asset: {e}") # Or use current_app.logger
        return None
    finally:
        if conn: conn.close()

def fetch_prompt_template_content(db_settings, template_id):
    """Fetches the content of a specific prompt template from the database."""
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT template_content FROM prompt_templates WHERE id = %s;",
            (template_id,)
        )
        result = cursor.fetchone()
        # Returns the template string if found, otherwise None
        return result[0] if result else None
    except psycopg2.Error as e:
        print(f"Database error fetching prompt template: {e}") # Or use current_app.logger
        return None
    finally:
        if conn: conn.close()

def load_user_preferences(db_config, username):
    """Loads all preferences for a given user."""
    conn = None
    prefs = {}
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        query = "SELECT usrpref_name, usrpref_value FROM usrpref WHERE usrpref_username = %s;"
        cursor.execute(query, (username,))
        for row in cursor.fetchall():
            prefs[row[0]] = row[1]
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error loading user preferences: {e}")
    finally:
        if conn: conn.close()
    return prefs

def fetch_runs_by_ids(db_config, run_ids, accessible_company_ids):
    """
    Fetches and decrypts runs by leveraging the new database functions for
    pgcrypto and handling KMS decryption in the application layer.
    """
    conn = None
    runs = []
    config = load_trends_config()

    if not accessible_company_ids or not run_ids:
        return []

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query = """
            SELECT
                hcr.id, hcr.run_timestamp, hcr.target_host, hcr.target_port,
                hcr.target_db_name, hcr.encryption_mode, hcr.encrypted_data_key,
                decrypted.decrypted_findings
            FROM health_check_runs hcr
            JOIN decrypt_run_findings(%(run_ids)s) AS decrypted ON hcr.id = decrypted.run_id
            WHERE hcr.id = ANY(%(run_ids)s) AND hcr.company_id = ANY(%(company_ids)s);
        """
        cursor.execute(query, {'run_ids': run_ids, 'company_ids': accessible_company_ids})
        
        for row in cursor.fetchall():
            findings_data = row['decrypted_findings']
            
            if row['encryption_mode'] == 'kms':
                try:
                    encrypted_blob = findings_data.encode('utf-8')
                    plaintext_key = decrypt_kms_data_key(row['encrypted_data_key'], config)
                    cipher_suite = Fernet(plaintext_key)
                    decrypted_json_text = cipher_suite.decrypt(encrypted_blob).decode('utf-8')
                    findings_data = json.loads(decrypted_json_text)
                except Exception as e:
                    current_app.logger.error(f"Failed to decrypt KMS data for run {row['id']}: {e}")
                    findings_data = {"error": "Failed to decrypt KMS data."}

            runs.append({
                "run_timestamp": row['run_timestamp'].isoformat(),
                "findings": findings_data,
                "target_host": row['target_host'],
                "target_port": row['target_port'],
                "target_db_name": row['target_db_name']
            })

    except Exception as e:
        current_app.logger.error(f"Error fetching or decrypting runs: {e}")
    finally:
        if conn: conn.close()
    
    return sorted(runs, key=lambda x: x['run_timestamp'], reverse=True)
