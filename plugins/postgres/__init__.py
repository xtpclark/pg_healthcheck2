import importlib.util
from pathlib import Path
import json

# --- Import the components of this plugin ---
from .connector import PostgresConnector
# No longer importing the static rules config: from .rules.analysis_rules import METRIC_ANALYSIS_CONFIG

# --- Import the base class it must implement ---
from plugins.base import BasePlugin


class PostgresPlugin(BasePlugin):
    """The PostgreSQL implementation of the plugin interface."""

    @property
    def technology_name(self):
        return "postgres"

    def get_connector(self, settings):
        """Returns an instance of the PostgreSQL connector."""
        return PostgresConnector(settings)

    def get_rules_config(self):
        """
        Dynamically discovers and loads all .json rule files
        from the 'rules' directory.
        """
        all_rules = {}
        # Assumes this script is in plugins/postgres/
        rules_dir = Path(__file__).parent / 'rules'

        if not rules_dir.is_dir():
            print(f"⚠️ Warning: Rules directory not found at {rules_dir}")
            return {}

        # Iterate over every .json file in the rules directory
        for rule_file in rules_dir.glob('*.json'):
            try:
                with open(rule_file, 'r') as f:
                    # Use the standard, secure json loader
                    loaded_rules = json.load(f)
                    all_rules.update(loaded_rules)
            except json.JSONDecodeError as e:
                # Catch specific JSON parsing errors
                print(f"⚠️ Warning: Could not parse rule file {rule_file.name}. Error: {e}")
            except IOError as e:
                # Catch file reading errors
                print(f"⚠️ Warning: Could not read rule file {rule_file.name}. Error: {e}")

        return all_rules

    def get_template_path(self) -> Path:
        """Returns the path to this plugin's templates directory."""
        return Path(__file__).parent / "templates"

    def get_report_definition(self, report_config_file=None):
        """
        Dynamically loads a report definition from a file.
        Falls back to the default if no file is specified.
        """
        if report_config_file:
            config_path = Path(report_config_file)
        else:
            # Default location for the standard report definition
            config_path = Path(__file__).parent / "reports" / "default.py"

        if not config_path.is_file():
            raise FileNotFoundError(f"Report configuration file not found: {config_path}")

        # Dynamically load the module from the specified path
        spec = importlib.util.spec_from_file_location("report_config_module", config_path)
        report_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(report_module)
        
        return getattr(report_module, 'REPORT_SECTIONS')
