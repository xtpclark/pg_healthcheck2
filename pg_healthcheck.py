#!/usr/bin/env python3
import yaml
import sys
import psycopg2
import csv
import subprocess
import importlib
import inspect # Import inspect to check function signatures
from pathlib import Path
from datetime import datetime # Import datetime type
from report_config import REPORT_SECTIONS
import json # Import json for structured data output
from decimal import Decimal # Import Decimal type

# Custom JSON Encoder to handle Decimal and datetime objects
class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj) # Convert Decimal to float
        if isinstance(obj, datetime):
            return obj.isoformat() # Convert datetime to ISO 8601 string
        # Let the base class default method raise the TypeError for other types
        return json.JSONEncoder.default(self, obj)

class HealthCheck:
    def __init__(self, config_file):
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
                settings = yaml.safe_load(f)
            if 'is_aurora' not in settings:
                settings['is_aurora'] = 'true' if 'aurora' in settings.get('host', '').lower() else 'false'
            
            # Ensure ai_analyze is a boolean, default to False if not specified
            settings['ai_analyze'] = settings.get('ai_analyze', False)
            
            # Read generic AI settings
            settings['ai_user'] = settings.get('ai_user', 'anonymous')
            settings['ai_api_key'] = settings.get('ai_api_key', '')
            settings['ai_endpoint'] = settings.get('ai_endpoint', 'https://generativelanguage.googleapis.com/v1beta/models/') # Default for Gemini
            settings['ai_model'] = settings.get('ai_model', 'gemini-2.0-flash') # Default for Gemini

            return settings
        except FileNotFoundError:
            print(f"Error: Config file {config_file} not found.")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"Error parsing config.yaml: {e}")
            sys.exit(1)

    def get_paths(self):
        """Set up paths for modules, comments, and output."""
        workdir = Path.cwd()
        return {
            'modules': workdir / 'modules',
            'comments': workdir / 'comments',
            'adoc_out': workdir / 'adoc_out' / self.settings['company_name'].lower(),
            'hist_out': workdir / 'health_history' / 'csv_out' / self.settings['company_name'].lower() / datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
            'adoc_image': workdir / 'adoc_out' / self.settings['company_name'].lower() / 'images'
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
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            self.cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements');")
            self.settings['has_pgstat'] = 't' if self.cursor.fetchone()[0] else 'f'
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            sys.exit(1)

    def execute_query(self, query, params=None, is_check=False, return_raw=False):
        """
        Execute a query and return formatted results.
        If return_raw is True, returns a tuple (formatted_string, list_of_dicts).
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
            if section.get('title'):
                self.adoc_content.append({'type': 'text', 'content': f"== {section['title'].replace('${PGDB}', self.settings['database'])}"})
            for action in section['actions']:
                if action['type'] == 'module':
                    # Check the 'ai_analyze' condition here for the run_recommendation module
                    if action['module'] == 'run_recommendation' and not self.settings.get('ai_analyze', False):
                        self.adoc_content.append({'type': 'text', 'content': "[NOTE]\n====\nAI analysis skipped as 'ai_analyze' is set to false in config.yaml.\n====\n"})
                        self.all_structured_findings['run_recommendation'] = {"status": "skipped", "note": "AI analysis skipped by configuration."}
                        continue # Skip running the module
                    
                    if action.get('condition') and not self.settings.get(action['condition']['var'].lower()) == action['condition']['value']:
                        continue
                    
                    content = self.run_module(action['module'], action['function'])
                    self.adoc_content.append({'type': 'text', 'content': content})
                elif action['type'] == 'comments':
                    content = self.read_comments_file(action['file']) if action['file'] != 'background.txt' else self.settings.get('background', '')
                    self.adoc_content.append({'type': 'text', 'content': f"== {action['file'].replace('.txt', '').title()}\n{content}\n"})
                elif action['type'] == 'image':
                    self.adoc_content.append({'type': 'text', 'content': f"image::{self.paths['adoc_image']}/{action['file']}[{action['alt']},300,300]"})
        
        # After all modules run, you can save self.all_structured_findings here
        # For example, save to a JSON file for later AI analysis
        structured_output_path = self.paths['adoc_out'] / "structured_health_check_findings.json"
        try:
            # Use the custom encoder here
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
