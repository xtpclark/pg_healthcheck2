from pathlib import Path
from plugins.base import BasePlugin
from .connector import ClickhouseConnector

class ClickhousePlugin(BasePlugin):
    """The Clickhouse implementation of the plugin interface."""

    @property
    def technology_name(self):
        return "clickhouse"

    def get_connector(self, settings):
        """Returns an instance of the Clickhouse connector."""
        return ClickhouseConnector(settings)

    def get_rules_config(self):
        """Returns the technology-specific analysis rules."""
        # Placeholder: Implement rule loading logic
        return {}
    
    def get_report_definition(self, report_config_file=None):
        """Returns the structure of the report, defining which checks to run."""
        # Placeholder: Implement report definition loading
        return []

    def get_template_path(self) -> Path:
        """Returns the path to this plugin's templates directory."""
        return Path(__file__).parent / "templates"
