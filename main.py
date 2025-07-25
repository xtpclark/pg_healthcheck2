#!/usr/bin/env python3
import yaml
import sys
import importlib
from pathlib import Path
from datetime import datetime, timedelta
import json
from decimal import Decimal
import re
import logging
import argparse
import pkgutil

from utils.dynamic_prompt_generator import generate_dynamic_prompt
from utils.run_recommendation import run_recommendation
from utils.report_builder import ReportBuilder
from plugins.base import BasePlugin
# New: Import the trend shipper module
from output_handlers import trend_shipper

# --- NEW: Define the application version by reading the VERSION file ---
try:
    APP_VERSION = (Path(__file__).parent / "VERSION").read_text().strip()
except FileNotFoundError:
    APP_VERSION = "unknown"

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
            try:
                module = importlib.import_module(f'plugins.{name}')
                for item_name in dir(module):
                    item = getattr(module, item_name)
                    if isinstance(item, type) and issubclass(item, BasePlugin) and item is not BasePlugin:
                        try:
                            plugin_instance = item()
                            discovered_plugins[plugin_instance.technology_name] = plugin_instance
                            print(f"✅ Discovered and loaded plugin: {plugin_instance.technology_name}")
                        except Exception as e:
                            print(f"⚠️  Warning: Could not instantiate plugin '{name}'. Error: {e}. Skipping.")
            except ImportError as e:
                print(f"⚠️  Warning: Could not import plugin '{name}'. Missing dependency: {e}. Skipping.")
            except Exception as e:
                print(f"⚠️  Warning: Failed to load plugin '{name}' due to an unexpected error: {e}. Skipping.")
    return discovered_plugins

class HealthCheck:
    def __init__(self, config_file, report_config_file=None):
        self.settings = self.load_settings(config_file)
        self.app_version = APP_VERSION # <-- Store the app version
        self.available_plugins = discover_plugins()
        active_tech = self.settings.get('db_type')
        self.active_plugin = self.available_plugins.get(active_tech)

        if not self.active_plugin:
            raise ValueError(f"Unsupported or missing db_type: '{active_tech}'. Available plugins: {list(self.available_plugins.keys())}")

        self.report_sections = self.active_plugin.get_report_definition(report_config_file)
        self.connector = self.active_plugin.get_connector(self.settings)
        self.paths = self.get_paths()
        self.adoc_content = ""
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

    def run_report(self):
        """Orchestrates the health check process."""
        self.connector.connect()

        # Pass the app_version to the ReportBuilder
        builder = ReportBuilder(self.connector, self.settings, self.active_plugin, self.report_sections, self.app_version)
        self.adoc_content, self.all_structured_findings = builder.build()

        if self.settings.get('ai_analyze', False):
            self.run_ai_analysis()

        # --- New: Call the Trend Shipper Module ---
        # After all checks are complete, pass the aggregated findings to the shipper.
        # This is wrapped in a try/except to ensure shipper failures don't stop the main tool.
        try:
            # We can add a check here for a setting in config.yaml if we want to disable this feature
            # For now, it will run if the module is present.
            print("\n--- Handing off findings to Trend Shipper ---")
            trend_shipper.run(self.all_structured_findings)
        except Exception as e:
            print(f"CRITICAL: The trend shipper module failed with an unexpected error: {e}")
        # ---------------------------------------------

        self.save_structured_findings()
        self.connector.disconnect()

    def run_ai_analysis(self):
        print("\n--- Starting AI Analysis ---")
        analysis_rules = self.active_plugin.get_rules_config()
        db_metadata = self.connector.get_db_metadata()
        db_version = db_metadata.get('version', 'N/A')
        db_name = db_metadata.get('db_name', self.settings.get('database', 'N/A'))

        dynamic_analysis = generate_dynamic_prompt(self.all_structured_findings, self.settings, analysis_rules, db_version, db_name, self.active_plugin)
        full_prompt = dynamic_analysis['prompt']
        
        ai_adoc, _ = run_recommendation(self.settings, full_prompt)
        self.adoc_content += f"\n\n{ai_adoc}"

    def save_structured_findings(self):
        # --- NEW: Add the app version to the structured data ---
        self.all_structured_findings['application_version'] = self.app_version
        
        output_path = self.paths['adoc_out'] / "structured_health_check_findings.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(self.all_structured_findings, f, indent=2, cls=CustomJsonEncoder)
        print(f"\nStructured health check findings saved to: {output_path}")

    def write_adoc(self, output_file):
        output_path = self.paths['adoc_out'] / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            f.write(self.adoc_content)

def main():
    parser = argparse.ArgumentParser(description='Database Health Check Tool')
    parser.add_argument('--config', default='config/config.yaml', help='Path to configuration file')
    parser.add_argument('--report-config', help='Path to a custom report configuration file.')
    parser.add_argument('--output', default='health_check.adoc', help='Output file name')
    args = parser.parse_args()
    
    print(f"--- Running Health Check Tool v{APP_VERSION} ---") # <-- Added version to startup message
    health_check = HealthCheck(args.config, args.report_config)
    
    # --- New: Check generate_report flag from config ---
    # Load settings to check the flag before running the full report builder
    settings = health_check.load_settings(args.config)
    generate_report_flag = settings.get('generate_report', True)
    
    health_check.run_report()

    if generate_report_flag:
        health_check.write_adoc(args.output)
        print(f"\nHealth check completed successfully!")
        print(f"Report generated: {health_check.paths['adoc_out'] / args.output}")
    else:
        print("\nHealth check data collection completed. AsciiDoc report generation was skipped as per 'generate_report: false' in config.")


if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).parent))
    main()
