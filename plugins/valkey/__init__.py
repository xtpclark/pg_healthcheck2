from pathlib import Path
from plugins.base import BasePlugin
from .connector import ValkeyConnector
from .reports.default import get_report_definition as get_valkey_report_definition

class ValkeyPlugin(BasePlugin):
    @property
    def technology_name(self):
        return "valkey"

    def get_connector(self, settings):
        return ValkeyConnector(settings)

    def get_report_definition(self, report_config_file=None):
        return get_valkey_report_definition()

    def get_rules_config(self):
        return {} # Placeholder

    def get_template_path(self):
        return Path(__file__).parent.parent / "valkey" / "templates"
