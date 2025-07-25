import importlib.util
from pathlib import Path

# --- Import the components of this plugin ---
from .connector import PostgresConnector
from .rules.analysis_rules import METRIC_ANALYSIS_CONFIG

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
        """Returns the PostgreSQL-specific analysis rules."""
        return METRIC_ANALYSIS_CONFIG

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

