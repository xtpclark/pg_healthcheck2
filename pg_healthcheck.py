#!/usr/bin/env python3
import yaml
import sys
import psycopg2
import csv
import subprocess
import importlib
from pathlib import Path
from datetime import datetime
from report_config import REPORT_SECTIONS

class HealthCheck:
    def __init__(self, config_file):
        self.settings = self.load_settings(config_file)
        self.paths = self.get_paths()
        self.adoc_content = []
        self.conn = None
        self.cursor = None

    def load_settings(self, config_file):
        """Load configuration from config.yaml."""
        try:
            with open(config_file, 'r') as f:
                settings = yaml.safe_load(f)
            if 'is_aurora' not in settings:
                settings['is_aurora'] = 'true' if 'aurora' in settings.get('host', '').lower() else 'false'
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

    def execute_query(self, query, params=None, is_check=False):
        """Execute a query and return formatted results."""
        try:
            if params is not None:
                if isinstance(params, dict) and '%(' in query:
                    self.cursor.execute(query, params)
                elif isinstance(params, (list, tuple)):
                    self.cursor.execute(query, params)
                else:
                    print(f"Warning: Invalid params type {type(params)} for query: {query[:100]}... Falling back to no params.")
                    self.cursor.execute(query)
            else:
                self.cursor.execute(query)
            if is_check:
                return str(self.cursor.fetchone()[0]) if self.cursor.rowcount > 0 else ""
            columns = [desc[0] for desc in self.cursor.description]
            results = self.cursor.fetchall()
            if not results:
                return "[NOTE]\n====\nNo results returned.\n====\n"
            table = ['|===', '|' + '|'.join(columns)]
            for row in results:
                table.append('|' + '|'.join(str(v) for v in row))
            table.append('|===')
            return '\n'.join(table)
        except psycopg2.Error as e:
            self.conn.rollback()
            return f"[ERROR]\n====\nQuery failed: {e}\n====\n"

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
        """Run a module function and return AsciiDoc content."""
        try:
            module = importlib.import_module(f"modules.{module_name}")
            func = getattr(module, function_name)
            return func(self.cursor, self.settings, self.execute_query, self.execute_pgbouncer)
        except (ImportError, AttributeError) as e:
            return f"[ERROR]\n====\nModule {module_name}.{function_name} failed: {e}\n====\n"

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
                    if action.get('condition') and not self.settings.get(action['condition']['var'].lower()) == action['condition']['value']:
                        continue
                    content = self.run_module(action['module'], action['function'])
                    self.adoc_content.append({'type': 'text', 'content': content})
                elif action['type'] == 'comments':
                    content = self.read_comments_file(action['file']) if action['file'] != 'background.txt' else self.settings.get('background', '')
                    self.adoc_content.append({'type': 'text', 'content': f"== {action['file'].replace('.txt', '').title()}\n{content}\n"})
                elif action['type'] == 'image':
                    self.adoc_content.append({'type': 'text', 'content': f"image::{self.paths['adoc_image']}/{action['file']}[{action['alt']},300,300]"})
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
