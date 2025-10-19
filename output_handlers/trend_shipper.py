"""Handles shipping of health check results to a persistent backend.

This module is responsible for taking the final results of a health check
and sending them to a storage destination for trend analysis. It is driven
by the `config/trends.yaml` file and acts as a dispatcher, supporting
different backends like PostgreSQL or a generic HTTP API.
"""

import yaml
import psycopg2
import requests
import json
from decimal import Decimal
from datetime import datetime, timedelta
from utils.json_utils import UniversalJSONEncoder, safe_json_dumps

def load_config(config_path='config/trends.yaml'):
    """Loads the trend shipper configuration from a YAML file.

    Args:
        config_path (str, optional): The path to the trend shipper
            configuration file. Defaults to 'config/trends.yaml'.

    Returns:
        dict | None: A dictionary with the configuration if the file is
        found and valid, otherwise None.
    """

    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print("Log: trends.yaml not found. Skipping trend analysis.")
        return None
    except Exception as e:
        print(f"Error loading trends.yaml: {e}")
        return None

def ship_to_database(db_config, target_info, findings_json, structured_findings, adoc_content):
    """Connects to PostgreSQL and inserts the health check data.

    This function handles the entire database transaction, including creating
    a company record if it doesn't exist and inserting the full findings,
    report, and execution context into the `health_check_runs` table.

    Args:
        db_config (dict): Database connection parameters for psycopg2.
        target_info (dict): Information about the target system being analyzed.
        findings_json (str): The complete structured findings, pre-serialized
            as a JSON string for database insertion.
        structured_findings (dict): The structured findings as a Python dict,
            used to extract execution context metadata.
        adoc_content (str): The full AsciiDoc report content to be stored.

    Returns:
        None
    """

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("Log: Successfully connected to PostgreSQL for trend shipping.")

        company_name = target_info.get('company_name', 'Default Company')
        cursor.execute("SELECT get_or_create_company(%s);", (company_name,))
        company_id = cursor.fetchone()[0]

        db_type = target_info.get('db_type', 'unknown')
        host = target_info.get('host', 'unknown')
        port = target_info.get('port', 0)
        database = target_info.get('database', 'unknown')
        
        # Extract execution context from the structured findings
        context = structured_findings.get('execution_context', {})
        run_by_user = context.get('run_by_user', 'unknown')
        run_from_host = context.get('run_from_host', 'unknown')
        tool_version = context.get('tool_version', 'unknown')
        prompt_template_name = structured_findings.get('prompt_template_name')

        ai_context = context.get('ai_execution_metrics')
        ai_context_json = json.dumps(ai_context) if ai_context else None

        # Updated insert query to include all new columns
        insert_query = """
        INSERT INTO health_check_runs (
            company_id, db_technology, target_host, target_port, target_db_name, 
            findings, prompt_template_name, run_by_user, run_from_host, tool_version,
            report_adoc, ai_execution_context
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(insert_query, (
            company_id, db_type, host, port, database, findings_json, 
            prompt_template_name, run_by_user, run_from_host, tool_version,
            adoc_content,  ai_context_json
        ))

        conn.commit()
        print("Log: Successfully shipped all findings and the AsciiDoc report to the database.")

    except psycopg2.Error as e:
        print(f"Error: Failed to ship data to PostgreSQL. {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def ship_to_api(api_config, target_info, findings, adoc_content):
    """Sends health check data and the AsciiDoc report to an API endpoint.

    Args:
        api_config (dict): Configuration for the API, including the `endpoint_url`.
        target_info (dict): Information about the target system.
        findings (dict): The complete structured findings dictionary.
        adoc_content (str): The full AsciiDoc report content.

    Returns:
        None
    """

    try:
        headers = {'Content-Type': 'application/json'}
        full_payload = {
            'target_info': target_info, 
            'findings': findings,
            'report_adoc': adoc_content
        }
        response = requests.post(api_config['endpoint_url'], headers=headers, data=safe_json_dumps(full_payload), timeout=15)
        response.raise_for_status()
        print(f"Log: Successfully sent raw findings to API. Status: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to ship data to API. {e}")

def run(structured_findings, target_info, adoc_content=None):
    """Main entry point for the trend shipper module.

    This function is called by the main application after a health check is
    complete. It loads the `trends.yaml` configuration and, based on the
    specified `destination`, calls the appropriate function to ship the results.

    Args:
        structured_findings (dict): The final dictionary of all structured
            findings from the health check.
        target_info (dict): A dictionary containing metadata about the target
            system (e.g., host, db_type, company_name).
        adoc_content (str, optional): The full AsciiDoc report. Defaults to None.

    Returns:
        None
    """

    print("--- Trend Shipper Module Started ---")
    config = load_config()
    if not config:
        print("--- Trend Shipper Module Finished (No Config) ---")
        return

    destination = config.get('destination')

    if destination == "postgresql":
        findings_as_json = safe_json_dumps(structured_findings)
        ship_to_database(config.get('database'), target_info, findings_as_json, structured_findings, adoc_content)
    elif destination == "api":
        ship_to_api(config.get('api'), target_info, structured_findings, adoc_content)
    else:
        print(f"Error: Unknown trend storage destination '{destination}'.")
        
    print("--- Trend Shipper Module Finished ---")
