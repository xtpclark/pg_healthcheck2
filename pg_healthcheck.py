#!/usr/bin/env python3
import yaml
import sys
import psycopg2
import csv
import subprocess
import importlib
import inspect
from pathlib import Path
from datetime import datetime, timedelta
import json
from decimal import Decimal
import re
import logging
import argparse

class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        if isinstance(obj, datetime): return obj.isoformat()
        if isinstance(obj, timedelta): return obj.total_seconds()
        return json.JSONEncoder.default(self, obj)

class HealthCheck:
    def __init__(self, config_file, report_config_file='report_config/report_config.py'):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('health_check.log'), logging.StreamHandler()])
        self.logger = logging.getLogger(__name__)
        self.report_sections = self.load_report_config(report_config_file)
        self.settings = self.load_settings(config_file)
        self.paths = self.get_paths()
        self.adoc_content = []
        self.all_structured_findings = {}
        self.conn = None
        self.cursor = None

    def load_settings(self, config_file):
        try:
            with open(config_file, 'r') as f:
                self.settings = yaml.safe_load(f)
            
            self.settings['is_aurora'] = bool(self.settings.get('is_aurora', False))
            self.settings['ai_analyze'] = bool(self.settings.get('ai_analyze', False))
            self.settings['using_connection_pooler'] = bool(self.settings.get('using_connection_pooler', False))
            self.settings['prompt_template'] = self.settings.get('prompt_template', 'prompt_template.j2')
            self.settings['ai_max_prompt_tokens'] = self.settings.get('ai_max_prompt_tokens', 8000)
            
            # Load other AI settings with defaults
            self.settings.setdefault('ai_api_key', '')
            self.settings.setdefault('ai_endpoint', 'https://generativelanguage.googleapis.com/v1beta/models/')
            self.settings.setdefault('ai_model', 'gemini-2.0-flash')
            self.settings.setdefault('ai_user', 'anonymous')
            self.settings.setdefault('ai_run_integrated', True)
            self.settings.setdefault('ai_user_header', '')
            self.settings.setdefault('ssl_cert_path', '')
            self.settings.setdefault('ai_temperature', 0.7)
            self.settings.setdefault('ai_max_output_tokens', 2048)
            self.settings.setdefault('statement_timeout', 30000)

            required_settings = ['host', 'port', 'database', 'user', 'password', 'company_name']
            if any(s not in self.settings for s in required_settings):
                raise ValueError(f"Missing required settings in config.yaml: {[s for s in required_settings if s not in self.settings]}")
            
            return self.settings
        except (FileNotFoundError, yaml.YAMLError, ValueError) as e:
            print(f"Error loading settings from {config_file}: {e}")
            sys.exit(1)

    def load_report_config(self, report_config_file):
        try:
            spec = importlib.util.spec_from_file_location("report_config", report_config_file)
            report_config_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(report_config_module)
            return getattr(report_config_module, 'REPORT_SECTIONS')
        except (FileNotFoundError, AttributeError) as e:
            print(f"Error loading report config {report_config_file}: {e}")
            sys.exit(1)

    def get_paths(self):
        workdir = Path.cwd()
        sanitized_company_name = re.sub(r'\W+', '_', self.settings['company_name'].lower()).strip('_')
        return {
            'modules': workdir / 'modules',
            'comments': workdir / 'comments',
            'adoc_out': workdir / 'adoc_out' / sanitized_company_name
        }

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(host=self.settings['host'], port=self.settings['port'], dbname=self.settings['database'], user=self.settings['user'], password=self.settings['password'], options=f"-c statement_timeout={self.settings['statement_timeout']}")
            self.conn.autocommit = self.settings.get('autocommit', True)
            self.cursor = self.conn.cursor()
            self.cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements');")
            self.settings['has_pgstat'] = 't' if self.cursor.fetchone()[0] else 'f'
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            sys.exit(1)

    def execute_query(self, query, params=None, is_check=False, return_raw=False):
        try:
            self.cursor.execute(query, params)
            if is_check:
                result = self.cursor.fetchone()[0] if self.cursor.rowcount > 0 else ""
                return (str(result), result) if return_raw else str(result)
            
            columns = [desc[0] for desc in self.cursor.description]
            results = self.cursor.fetchall()
            raw_results = [dict(zip(columns, row)) for row in results]

            if not results:
                formatted = "[NOTE]\n====\nNo results returned.\n====\n"
                return (formatted, []) if return_raw else formatted

            table = ['|===', '|' + '|'.join(columns)]
            table.extend('|' + '|'.join(str(v) for v in row) for row in results)
            table.append('|===')
            formatted = '\n'.join(table)
            
            return (formatted, raw_results) if return_raw else formatted
        except psycopg2.Error as e:
            self.conn.rollback()
            error_str = f"[ERROR]\n====\nQuery failed: {e}\n====\n"
            return (error_str, {"error": str(e), "query": query}) if return_raw else error_str

    def execute_pgbouncer(self, command):
        try:
            result = subprocess.run(f"{self.settings['pgbouncer_cmd']} -c \"{command}\"", shell=True, capture_output=True, text=True, check=True)
            return result.stdout
        except subprocess.SubprocessError as e:
            return f"[ERROR]\n====\nPgBouncer command failed: {e}\n====\n"

    def read_comments_file(self, comments_file):
        try:
            with open(self.paths['comments'] / comments_file, 'r') as f:
                content = f.read()
                for key, value in self.settings.items():
                    content = content.replace(f'${key.upper()}', str(value))
                return content
        except FileNotFoundError:
            return f"[ERROR]\n====\nComments file {comments_file} not found.\n====\n"

    def run_module(self, module_name, function_name):
        try:
            module = importlib.import_module(f"modules.{module_name}")
            func = getattr(module, function_name)
            func_sig = inspect.signature(func)
            func_args = [self.cursor, self.settings, self.execute_query, self.execute_pgbouncer]
            if 'all_structured_findings' in func_sig.parameters:
                func_args.append(self.all_structured_findings)
            
            module_output = func(*func_args)

            if isinstance(module_output, tuple) and len(module_output) == 2:
                adoc_content, structured_data = module_output
                # MODIFIED: The AI module now handles its own data structure.
                # For all other modules, we save their structured output.
                if module_name != 'run_recommendation_enhanced':
                    self.all_structured_findings[module_name] = {"status": "success", "data": structured_data}
                return adoc_content
            else:
                self.all_structured_findings[module_name] = {"status": "warning", "note": "Module not yet refactored for structured output."}
                return module_output
        except Exception as e:
            error_msg = f"[ERROR]\n====\nModule {module_name}.{function_name} failed: {e}\n====\n"
            self.all_structured_findings[module_name] = {"status": "error", "error": str(e)}
            return error_msg

    def run_report(self):
        self.connect_db()
        for section in self.report_sections:
            if section.get('condition') and not self.settings.get(section['condition']['var'].lower()) == section['condition']['value']:
                continue
            
            if section['actions'][0]['type'] == 'header':
                self.adoc_content.append({'type': 'text', 'content': self.read_comments_file(section['actions'][0]['file'])})
                continue

            self.adoc_content.append({'type': 'text', 'content': f"== {section['title'].replace('${PGDB}', self.settings['database'])}"})
            
            for action in section['actions']:
                if action.get('condition') and not self.settings.get(action['condition']['var'].lower()) == action['condition']['value']:
                    continue
                
                if action['type'] == 'module':
                    content = self.run_module(action['module'], action['function'])
                    self.adoc_content.append({'type': 'text', 'content': content})
                elif action['type'] == 'comments':
                    content = self.read_comments_file(action['file'])
                    self.adoc_content.append({'type': 'text', 'content': content})

        # MODIFIED: Save only the raw structured findings. Prompt generation is now offline.
        structured_output_path = self.paths['adoc_out'] / "structured_health_check_findings.json"
        try:
            structured_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(structured_output_path, 'w') as f:
                json.dump(self.all_structured_findings, f, indent=2, cls=CustomJsonEncoder)
            print(f"\nStructured health check findings saved to: {structured_output_path}")
        except Exception as e:
            print(f"\nError saving structured findings: {e}")

        self.conn.close()

    def write_adoc(self, output_file):
        output_path = self.paths['adoc_out'] / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            for section in self.adoc_content:
                f.write(section['content'] + '\n\n')

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL Health Check Tool')
    parser.add_argument('--config', default='config/config.yaml', help='Path to configuration file')
    parser.add_argument('--report-config', default='report_config/report_config.py', help='Path to report configuration file')
    parser.add_argument('--output', default='health_check.adoc', help='Output file name')
    args = parser.parse_args()
    
    health_check = HealthCheck(args.config, args.report_config)
    health_check.run_report()
    health_check.write_adoc(args.output)
    
    print(f"\nHealth check completed successfully!")
    print(f"Report generated: {health_check.paths['adoc_out'] / args.output}")

if __name__ == '__main__':
    main()
