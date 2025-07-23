#!/usr/bin/env python3
import yaml
import sys
import importlib
import inspect
from pathlib import Path
from datetime import datetime, timedelta
import json
from decimal import Decimal
import re
import logging
import argparse
import pkgutil

# --- Import from the new utils directory ---
from utils.dynamic_prompt_generator import generate_dynamic_prompt
from utils.run_recommendation import run_recommendation
from plugins.base import BasePlugin

class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        if isinstance(obj, datetime): return obj.isoformat()
        if isinstance(obj, timedelta): return obj.total_seconds()
        return json.JSONEncoder.default(self, obj)

def discover_plugins():
    """Find and load all available plugins from the 'plugins' directory."""
    plugins_path = Path(__file__).parent / "plugins"
    discovered_plugins = {}
    for _, name, _ in pkgutil.iter_modules([str(plugins_path)]):
        if name != "base":
            module = importlib.import_module(f'plugins.{name}')
            for item_name in dir(module):
                item = getattr(module, item_name)
                if isinstance(item, type) and issubclass(item, BasePlugin) and item is not BasePlugin:
                    plugin_instance = item()
                    discovered_plugins[plugin_instance.technology_name] = plugin_instance
                    print(f"Discovered plugin: {plugin_instance.technology_name}")
    return discovered_plugins

class HealthCheck:
    def __init__(self, config_file, report_config_file=None):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('health_check.log'), logging.StreamHandler()])
        self.logger = logging.getLogger(__name__)
        self.settings = self.load_settings(config_file)

        self.available_plugins = discover_plugins()
        active_tech = self.settings.get('db_type')
        self.active_plugin = self.available_plugins.get(active_tech)

        if not self.active_plugin:
            raise ValueError(f"Unsupported or missing db_type: '{active_tech}'. Available plugins: {list(self.available_plugins.keys())}")

        self.report_sections = self.active_plugin.get_report_definition(report_config_file)
        self.connector = self.active_plugin.get_connector(self.settings)

        self.paths = self.get_paths()
        self.adoc_content = []
        self.all_structured_findings = {}

    def load_settings(self, config_file):
        try:
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        except (FileNotFoundError, yaml.YAMLError) as e:
            print(f"Error loading settings from {config_file}: {e}")
            sys.exit(1)

    def get_paths(self):
        workdir = Path.cwd()
        sanitized_company_name = re.sub(r'\W+', '_', self.settings['company_name'].lower()).strip('_')
        return { 'adoc_out': workdir / 'adoc_out' / sanitized_company_name }

    def read_comments_file(self, comments_file):
        try:
            plugin_dir = Path(inspect.getfile(self.active_plugin.__class__)).parent
            file_path = plugin_dir / "comments" / comments_file
            with open(file_path, 'r') as f:
                content = f.read()
            for key, value in self.settings.items():
                content = content.replace(f'${key.upper()}', str(value))
            return content
        except FileNotFoundError:
            return f"[ERROR]\n====\nComments file {comments_file} not found in plugin.\n====\n"

    def run_module(self, module_name, function_name):
        try:
            module = importlib.import_module(module_name)
            func = getattr(module, function_name)
            module_output = func(self.connector, self.settings)

            if isinstance(module_output, tuple) and len(module_output) == 2:
                adoc_content, structured_data = module_output
                self.all_structured_findings[module_name.split('.')[-1]] = {"status": "success", "data": structured_data}
                return adoc_content
            else:
                self.all_structured_findings[module_name.split('.')[-1]] = {"status": "warning", "note": "Module did not return structured data."}
                return module_output
        except Exception as e:
            error_msg = f"[ERROR]\n====\nModule {module_name}.{function_name} failed: {e}\n====\n"
            self.all_structured_findings[module_name.split('.')[-1]] = {"status": "error", "error": str(e)}
            return error_msg

    def run_report(self):
        self.connector.connect()

        for section in self.report_sections:
            # ... (report loop logic is correct)
            if section.get('title'): # Avoid printing empty titles
                 self.adoc_content.append(f"== {section['title']}")
            for action in section['actions']:
                 if action['type'] == 'module':
                     content = self.run_module(action['module'], action['function'])
                     self.adoc_content.append(content)
                 elif action['type'] == 'comments':
                     content = self.read_comments_file(action['file'])
                     self.adoc_content.append(content)


        if self.settings.get('ai_analyze', False):
            self.run_ai_analysis()

        output_path = self.paths['adoc_out'] / "structured_health_check_findings.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(self.all_structured_findings, f, indent=2, cls=CustomJsonEncoder)
        print(f"\nStructured health check findings saved to: {output_path}")

        self.connector.disconnect()

    def run_ai_analysis(self):
        print("\n--- Starting AI Analysis ---")
        analysis_rules = self.active_plugin.get_rules_config()
        
        # --- CORRECTED: Get metadata from the connector ---
        db_metadata = self.connector.get_db_metadata()
        db_version = db_metadata.get('version', 'N/A')
        db_name = db_metadata.get('db_name', self.settings.get('database', 'N/A'))

        dynamic_analysis = generate_dynamic_prompt(self.all_structured_findings, self.settings, analysis_rules, db_version, db_name)
        full_prompt = dynamic_analysis['prompt']

        ai_adoc, _ = run_recommendation(self.settings, full_prompt)
        self.adoc_content.append(ai_adoc)

    def write_adoc(self, output_file):
        output_path = self.paths['adoc_out'] / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            f.write('\n\n'.join(self.adoc_content))

def main():
    parser = argparse.ArgumentParser(description='Database Health Check Tool')
    parser.add_argument('--config', default='config/config.yaml', help='Path to configuration file')
    parser.add_argument('--report-config', help='Path to a custom report configuration file.')
    parser.add_argument('--output', default='health_check.adoc', help='Output file name')
    args = parser.parse_args()
    
    health_check = HealthCheck(args.config, args.report_config)
    health_check.run_report()
    health_check.write_adoc(args.output)
    
    print(f"\nHealth check completed successfully!")
    print(f"Report generated: {health_check.paths['adoc_out'] / args.output}")

if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).parent))
    main()
