#!/usr/bin/env python3
import yaml
import sys
import psycopg2
import csv
import subprocess
import importlib
import inspect # Import inspect to check function signatures
from pathlib import Path
from datetime import datetime, timedelta # Import datetime and timedelta types
from report_config import REPORT_SECTIONS
import json # Import json for structured data output
from decimal import Decimal # Import Decimal type
import re # Import re module for regular expressions
import logging # Import logging for better error tracking

# Custom JSON Encoder to handle Decimal and datetime objects
class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj) # Convert Decimal to float
        if isinstance(obj, datetime):
            return obj.isoformat() # Convert datetime to ISO 8601 string
        if isinstance(obj, timedelta): # Handle timedelta objects
            return obj.total_seconds() # Convert timedelta to total seconds
        # Let the base class default method raise the TypeError for other types
        return json.JSONEncoder.default(self, obj)

class HealthCheck:
    def __init__(self, config_file):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('health_check.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        self.settings = self.load_settings(config_file)
        self.paths = self.get_paths()
        self.adoc_content = []
        self.all_structured_findings = {} # New attribute to store structured data
        self.conn = None
        self.cursor = None

    def load_settings(self, config_file):
        """Load configuration from config.yaml."""
        try:
            with open(config_file, 'r') as f:
                self.settings = yaml.safe_load(f)
            # Standardize boolean settings
            if 'is_aurora' not in self.settings:
                self.settings['is_aurora'] = 'aurora' in self.settings.get('host', '').lower()
            else:
                self.settings['is_aurora'] = bool(self.settings['is_aurora'])
            
            # Ensure ai_analyze is a boolean, default to False if not specified
            self.settings['ai_analyze'] = bool(self.settings.get('ai_analyze', False))
            
            # Read generic AI settings
            self.settings['ai_api_key'] = self.settings.get('ai_api_key', '')
            self.settings['ai_endpoint'] = self.settings.get('ai_endpoint', 'https://generativelanguage.googleapis.com/v1beta/models/') # Default for Gemini
            self.settings['ai_model'] = self.settings.get('ai_model', 'gemini-2.0-flash') # Default for Gemini
            self.settings['ai_user'] = self.settings.get('ai_user', 'anonymous') # Default for AI user
            self.settings['ai_run_integrated'] = self.settings.get('ai_run_integrated', True) # Default to True for integrated run
            self.settings['ai_user_header'] = self.settings.get('ai_user_header', '') # Default to empty string
            self.settings['ssl_cert_path'] = self.settings.get('ssl_cert_path', '') # Load SSL cert path
            self.settings['ai_temperature'] = self.settings.get('ai_temperature', 0.7) # Load AI temperature, default 0.7
            self.settings['ai_max_output_tokens'] = self.settings.get('ai_max_output_tokens', 2048) # Load AI max output tokens, default 2048

            # Validate required settings
            required_settings = ['host', 'port', 'database', 'user', 'password', 'company_name']
            missing_settings = [setting for setting in required_settings if setting not in self.settings]
            if missing_settings:
                print(f"Error: Missing required settings in config.yaml: {missing_settings}")
                sys.exit(1)

            return self.settings
        except FileNotFoundError:
            print(f"Error: Config file {config_file} not found.")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"Error parsing config.yaml: {e}")
            sys.exit(1)

    def get_paths(self):
        """Set up paths for modules, comments, and output."""
        workdir = Path.cwd()
        # Sanitize company_name for use in file paths
        # Convert to lowercase, replace spaces with underscores, remove non-alphanumeric (except underscores)
        sanitized_company_name = re.sub(r'\W+', '_', self.settings['company_name'].lower()).strip('_')
        
        return {
            'modules': workdir / 'modules',
            'comments': workdir / 'comments',
            'adoc_out': workdir / 'adoc_out' / sanitized_company_name,
            'hist_out': workdir / 'health_history' / 'csv_out' / sanitized_company_name / datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
            'adoc_image': workdir / 'adoc_out' / sanitized_company_name / 'images'
        }

    def connect_db(self):
        """Connect to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                host=self.settings['host'],
                port=self.settings['port'],
                dbname=self.settings['database'],
                user=self.settings['user'],
                password=self.settings['password']
            )
            # Use autocommit for read-only health checks, but allow override
            self.conn.autocommit = self.settings.get('autocommit', True)
            self.cursor = self.conn.cursor()
            
            # NEW: More robust check for pg_stat_statements.
            # Try to query pg_stat_statements_info first (for newer PG versions), then pg_stat_statements.
            self.settings['has_pgstat'] = 'f' # Default to false
            try:
                self.cursor.execute("SELECT count(*) FROM pg_stat_statements_info;")
                self.settings['has_pgstat'] = 't'
            except psycopg2.Error:
                # If pg_stat_statements_info fails, try the older pg_stat_statements
                try:
                    self.cursor.execute("SELECT count(*) FROM pg_stat_statements;")
                    self.settings['has_pgstat'] = 't'
                except psycopg2.Error as e:
                    print(f"Warning: Neither pg_stat_statements_info nor pg_stat_statements view is accessible or enabled for data collection: {e}")
                    self.settings['has_pgstat'] = 'f'
            
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            if self.conn:
                self.conn.close()
            sys.exit(1)

    def execute_query(self, query, params=None, is_check=False, return_raw=False):
        """
        Execute a query and return formatted results.
        
        Args:
            query (str): SQL query to execute
            params (dict/list/tuple, optional): Query parameters for parameterized queries
            is_check (bool): If True, returns single value instead of table format
            return_raw (bool): If True, returns tuple (formatted_string, raw_data)
            
        Returns:
            str or tuple: Formatted AsciiDoc string, or (formatted_string, raw_data) if return_raw=True
            
        Note:
            - Supports both named (%(name)s) and positional (%s) parameter styles
            - Handles various parameter types (dict, list, tuple, None)
            - Returns structured error messages for failed queries
        """
        try:
            if params is not None:
                # Case 1: Named parameters (params is a dict and query contains named placeholders)
                if isinstance(params, dict) and '%(' in query:
                    self.cursor.execute(query, params)
                # Case 2: Positional parameters (params is a list or tuple)
                elif isinstance(params, (list, tuple)):
                    self.cursor.execute(query, params)
                # Case 3: params is not None, but not a valid dict for named params AND not a sequence.
                else:
                    print(f"Warning: Invalid or unhandled params type {type(params)} for query: {query[:100]}... Executing query without parameters.")
                    self.cursor.execute(query) 
            # Case 4: params is None, execute query without parameters.
            else:
                self.cursor.execute(query)

            if is_check:
                result_value = str(self.cursor.fetchone()[0]) if self.cursor.rowcount > 0 else ""
                if return_raw:
                    # For is_check, raw is just the value
                    return result_value, result_value 
                return result_value
            
            columns = [desc[0] for desc in self.cursor.description]
            results = self.cursor.fetchall()

            # Prepare raw results as a list of dictionaries
            raw_results_dicts = [dict(zip(columns, row)) for row in results]

            if not results:
                formatted_string = "[NOTE]\n====\nNo results returned.\n====\n"
                if return_raw:
                    return formatted_string, [] # Return empty list for raw if no results
                return formatted_string
            
            table = ['|===', '|' + '|'.join(columns)]
            for row in results:
                table.append('|' + '|'.join(str(v) for v in row))
            table.append('|===')
            formatted_string = '\n'.join(table)

            if return_raw:
                return formatted_string, raw_results_dicts
            return formatted_string
        except psycopg2.Error as e:
            self.conn.rollback()
            error_string = f"[ERROR]\n====\nQuery failed: {e}\n====\n"
            if return_raw:
                # For errors, store the error message and the query that failed
                return error_string, {"error": str(e), "query": query} 
            return error_string

    def execute_pgbouncer(self, command):
        """Execute a PgBouncer command."""
        try:
            result = subprocess.run(f"{self.settings['pgbouncer_cmd']} -c \"{command}\"", shell=True, capture_output=True, text=True)
            return result.stdout
        except subprocess.SubprocessError as e:
            return f"[ERROR]\n====\nPgBouncer command failed: {e}\n====\n"

    def read_comments_file(self, comments_file):
        """Read a comments file and return its content."""
        try:
            with open(self.paths['comments'] / comments_file, 'r') as f:
                content = f.read()
                for key, value in self.settings.items():
                    content = content.replace(f'${key.upper()}', str(value))
                return content
        except FileNotFoundError:
            return f"[ERROR]\n====\nComments file {comments_file} not found.\n====\n"

    def run_module(self, module_name, function_name):
        """
        Run a module function and handle its return.
        Supports modules returning (adoc_content, structured_data) or just adoc_content.
        """
        try:
            module = importlib.import_module(f"modules.{module_name}")
            func = getattr(module, function_name)
            
            # Inspect the function signature to determine if it accepts all_structured_findings
            func_signature = inspect.signature(func)
            
            # Prepare arguments to pass to the module function
            func_args = [self.cursor, self.settings, self.execute_query, self.execute_pgbouncer]
            if 'all_structured_findings' in func_signature.parameters:
                func_args.append(self.all_structured_findings)

            module_output = func(*func_args)

            # Handle the module's return: tuple (adoc_string, structured_data) or just adoc_string
            if isinstance(module_output, tuple) and len(module_output) == 2:
                adoc_content, structured_data = module_output
                self.all_structured_findings[module_name] = {"status": "success", "data": structured_data}
                return adoc_content
            else:
                self.all_structured_findings[module_name] = {"status": "warning", "note": "Module not yet refactored for structured output."}
                return module_output
        except (ImportError, AttributeError) as e:
            error_msg = f"[ERROR]\n====\nModule {module_name}.{function_name} failed: {e}\n====\n"
            self.all_structured_findings[module_name] = {"status": "error", "error": str(e), "details": "Failed to load or execute module."}
            return error_msg
        except Exception as e: # Catch any other unexpected errors from the module
            error_msg = f"[ERROR]\n====\nModule {module_name}.{function_name} failed unexpectedly: {e}\n====\n"
            self.all_structured_findings[module_name] = {"status": "error", "error": str(e), "details": "Unexpected error during module execution."}
            return error_msg


    def run_report(self):
        """Run the report generation process."""
        self.connect_db()
        for section in REPORT_SECTIONS:
            if section.get('condition') and not self.settings.get(section['condition']['var'].lower()) == section['condition']['value']:
                continue
            
            # Special handling for 'header' type
            if section['actions'] and section['actions'][0]['type'] == 'header':
                header_file = section['actions'][0]['file']
                content = self.read_comments_file(header_file)
                self.adoc_content.append({'type': 'text', 'content': content})
                continue # Skip adding section title and processing other actions for header section

            # Add section title for all sections except header
            if section.get('title'):
                self.adoc_content.append({'type': 'text', 'content': f"== {section['title'].replace('${PGDB}', self.settings['database'])}"})
            
            # Check if this section has only one module action
            module_actions = [action for action in section['actions'] if action['type'] == 'module']
            has_single_module = len(module_actions) == 1
            
            for action in section['actions']: # Iterate through actions within the section
                if action['type'] == 'module':
                    # Removed the 'ai_run_integrated' check here.
                    # run_recommendation.py will now always be called if ai_analyze is true.
                    # The decision to make the API call or not is moved into run_recommendation.py itself.
                    
                    if action.get('condition') and not self.settings.get(action['condition']['var'].lower()) == action['condition']['value']:
                        continue
                    
                    content = self.run_module(action['module'], action['function'])
                    self.adoc_content.append({'type': 'text', 'content': content})
                elif action['type'] == 'comments':
                    # Determine the display title for the comments section
                    display_title = action.get('display_title', action['file'].replace('.txt', '').title())
                    content = self.read_comments_file(action['file'])
                    
                    # Only add subheading if display_title is not empty AND this is not a best practices section
                    # Best practices sections should not have subheadings as they're all at the same level
                    if display_title.strip() and not any(bp in display_title.lower() for bp in ['best practices', 'practices']):
                        self.adoc_content.append({'type': 'text', 'content': f"=== {display_title}\n{content}\n"})
                    else:
                        self.adoc_content.append({'type': 'text', 'content': f"{content}\n"})
                elif action['type'] == 'image':
                    self.adoc_content.append({'type': 'text', 'content': f"image::{self.paths['adoc_image']}/{action['file']}[{action['alt']},300,300]"})
        
        # After all modules run, you can save self.all_structured_findings here
        # For example, save to a JSON file for later AI analysis
        structured_output_path = self.paths['adoc_out'] / "structured_health_check_findings.json"
        try:
            # Ensure the parent directory exists before writing the file
            structured_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(structured_output_path, 'w') as f:
                json.dump(self.all_structured_findings, f, indent=2, cls=CustomJsonEncoder)
            print(f"\nStructured health check findings saved to: {structured_output_path}")
        except Exception as e:
            print(f"\nError saving structured findings: {e}")

        self.conn.close()

    def write_adoc(self, output_file):
        """Write the AsciiDoc content to the output file."""
        output_path = self.paths['adoc_out'] / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            for section in self.adoc_content:
                if section['type'] == 'text':
                    f.write(section['content'] + '\n\n')
                elif section['type'] == 'page':
                    f.write('<<<\n\n')

def main():
    config_file = 'config/config.yaml'
    output_file = 'health_check.adoc'

    health_check = HealthCheck(config_file)
    health_check.run_report()
    health_check.write_adoc(output_file)

if __name__ == '__main__':
    main()
