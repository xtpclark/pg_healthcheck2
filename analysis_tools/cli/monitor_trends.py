# analysis_tools/cli/monitor_trends.py

import yaml
import psycopg2
import json
import argparse
from deepdiff import DeepDiff
import re
from datetime import datetime, timedelta

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

def fetch_runs_for_comparison(db_config, company_name, host=None, port=None, dbname=None, start_date=None, end_date=None):
    """
    Fetches two runs for comparison with smarter default logic.
    """
    conn = None
    runs = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # If host/port/dbname are not specified, we find the latest run first
        if not (host and port and dbname) and not (start_date or end_date):
            print("Info: No specific target provided. Finding the most recent run to use as a baseline...")
            latest_run_query = """
                SELECT run_timestamp, findings, target_host, target_port, target_db_name
                FROM health_check_runs
                WHERE company_name = %s
                ORDER BY run_timestamp DESC LIMIT 1;
            """
            cursor.execute(latest_run_query, (company_name,))
            latest_run_row = cursor.fetchone()

            if not latest_run_row:
                print(f"Found 0 health check runs for company '{company_name}'.")
                return []
            
            # Extract the target info from the latest run
            _, _, host, port, dbname = latest_run_row
            print(f"Info: Baseline is the database '{dbname}' on {host}:{port}.")
        
        # Now, build the main query with the exact target
        base_query = """
        SELECT run_timestamp, findings, target_host, target_port, target_db_name
        FROM health_check_runs
        WHERE company_name = %s AND target_host = %s AND target_port = %s AND target_db_name = %s
        """
        params = [company_name, host, port, dbname]

        if start_date:
            base_query += " AND run_timestamp >= %s"
            params.append(start_date)
        if end_date:
            base_query += " AND run_timestamp <= %s"
            params.append(end_date)

        if start_date or end_date:
            first_run_query = base_query + " ORDER BY run_timestamp ASC LIMIT 1;"
            cursor.execute(first_run_query, tuple(params))
            first_run = cursor.fetchone()
            last_run_query = base_query + " ORDER BY run_timestamp DESC LIMIT 1;"
            cursor.execute(last_run_query, tuple(params))
            last_run = cursor.fetchone()
            fetched_rows = []
            if last_run: fetched_rows.append(last_run)
            if first_run and (not last_run or first_run[0] != last_run[0]):
                fetched_rows.append(first_run)
        else:
            default_query = base_query + " ORDER BY run_timestamp DESC LIMIT 2;"
            cursor.execute(default_query, tuple(params))
            fetched_rows = cursor.fetchall()

        print(f"Found {len(fetched_rows)} health check runs to compare for the target instance.")

        for row in fetched_rows:
            run_timestamp, findings_json, r_host, r_port, r_dbname = row
            runs.append({
                "run_timestamp": run_timestamp.isoformat(),
                "findings": findings_json,
                "target_host": r_host, "target_port": r_port, "target_db_name": r_dbname
            })

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn: conn.close()
    return sorted(runs, key=lambda x: x['run_timestamp'], reverse=True)

def format_path(path):
    """Cleans up the deepdiff path for readability."""
    return re.sub(r"root\['(.*?)'\]", r'\1', path).replace("'", "")

def format_timedelta(delta):
    """Formats a timedelta object into a human-readable string."""
    parts = []
    if delta.days > 0:
        parts.append(f"{delta.days} day(s)")
    
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if hours > 0:
        parts.append(f"{hours} hour(s)")
    if minutes > 0:
        parts.append(f"{minutes} minute(s)")
    if seconds > 0 or not parts:
        parts.append(f"{seconds} second(s)")
        
    return ", ".join(parts)

def display_polished_summary(runs):
    """Compares the two runs and prints a summary."""
    if len(runs) < 2:
        print("\nInsufficient data for comparison. Need at least two runs matching the criteria.")
        return

    latest_run = runs[0]
    previous_run = runs[1]
    
    # Calculate elapsed time
    latest_ts = datetime.fromisoformat(latest_run['run_timestamp'])
    previous_ts = datetime.fromisoformat(previous_run['run_timestamp'])
    elapsed_time = format_timedelta(latest_ts - previous_ts)

    print("\n" + "="*80)
    print(f"🔍 Comparison Summary for {latest_run['target_host']}:{latest_run['target_port']}/{latest_run['target_db_name']}")
    print(f"  - Latest Run:   {latest_run['run_timestamp']}")
    print(f"  - Previous Run: {previous_run['run_timestamp']}")
    print(f"  - Elapsed Time: {elapsed_time}")
    print("="*80)

    diff = DeepDiff(previous_run['findings'], latest_run['findings'], ignore_order=True, view='tree')
    
    if not diff:
        print("\n✅ No changes detected between the two runs.")
    else:
        changes_by_check = {}
        for change_type, items in diff.items():
            for item in items:
                top_level_check = item.path(output_format='list')[0]
                if top_level_check not in changes_by_check:
                    changes_by_check[top_level_check] = []
                
                path = format_path(item.path())
                if change_type == 'values_changed':
                    changes_by_check[top_level_check].append(f"  🔄 Value changed at '{path}': '{item.t1}' -> '{item.t2}'")
                elif change_type == 'iterable_item_added':
                    changes_by_check[top_level_check].append(f"  ➕ Item added to list at '{path}': {json.dumps(item.t2)}")
                elif change_type == 'iterable_item_removed':
                    changes_by_check[top_level_check].append(f"  ➖ Item removed from list at '{path}': {json.dumps(item.t1)}")

        print("\n--- Detected Changes by Check ---")
        for check, changes in sorted(changes_by_check.items()):
            print(f"\n  * In '{check}':")
            for change_summary in changes:
                print(f"    {change_summary}")
    print("\n" + "="*80)

def main():
    parser = argparse.ArgumentParser(description='Monitor and analyze health check trends.')
    parser.add_argument('--company-name', type=str, required=True, help='The name of the company to analyze.')
    parser.add_argument('--host', type=str, help='(Optional) Filter by a specific database host.')
    parser.add_argument('--port', type=int, help='(Optional) Filter by a specific database port.')
    parser.add_argument('--dbname', type=str, help='(Optional) Filter by a specific database name.')
    parser.add_argument('--start-date', type=str, help='(Optional) The start of the date range to compare (YYYY-MM-DD).')
    parser.add_argument('--end-date', type=str, help='(Optional) The end of the date range to compare (YYYY-MM-DD).')
    args = parser.parse_args()

    config = load_trends_config()
    if not config: return
        
    db_settings = config.get('database')
    
    runs = fetch_runs_for_comparison(db_settings, args.company_name, args.host, args.port, args.dbname, args.start_date, args.end_date)
    display_polished_summary(runs)

if __name__ == '__main__':
    main()
