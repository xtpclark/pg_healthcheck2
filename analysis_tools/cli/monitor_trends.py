# analysis_tools/cli/monitor_trends.py

import yaml
import psycopg2
import json
import argparse
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

def fetch_runs(db_config, company_name, limit=2):
    """Fetches the latest runs for a company from the simplified schema."""
    conn = None
    runs = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Updated query for the new schema, filtering by company_name
        query = """
        SELECT run_timestamp, findings
        FROM health_check_runs
        WHERE company_name = %s
        ORDER BY run_timestamp DESC
        LIMIT %s;
        """
        cursor.execute(query, (company_name, limit))
        rows = cursor.fetchall()
        print(f"Found {len(rows)} health check runs for company '{company_name}'.")

        for row in rows:
            run_timestamp, findings_json = row
            # Data is already unencrypted
            decrypted_run = {
                "run_timestamp": run_timestamp.isoformat(),
                "findings": findings_json # The column directly contains the JSON
            }
            runs.append(decrypted_run)

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
    return runs

def format_path(path):
    """Cleans up the deepdiff path for readability."""
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

    for change_type, items in diff.items():
        for item in items:
            top_level_check = item.path(output_format='list')[0]
            if top_level_check not in changes_by_check:
                changes_by_check[top_level_check] = []
            
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
            elif change_type == 'iterable_item_removed':
                 path = format_path(item.path())
                 changes_by_check[top_level_check].append(
                    f"  ➖ Item removed from list at '{path}': {json.dumps(item.t1)}"
                 )

    print("\n--- Detected Changes by Check ---")
    for check, changes in sorted(changes_by_check.items()):
        print(f"\n  * In '{check}':")
        for change_summary in changes:
            print(f"    {change_summary}")

    print("\n" + "="*80)

def main():
    parser = argparse.ArgumentParser(description='Monitor and analyze health check trends.')
    parser.add_argument('--company-name', type=str, required=True, help='The name of the company to analyze.')
    args = parser.parse_args()

    config = load_trends_config()
    if not config: return
        
    db_settings = config.get('database')
    
    runs = fetch_runs(db_settings, args.company_name, limit=2)
    display_polished_summary(runs)

if __name__ == '__main__':
    main()
