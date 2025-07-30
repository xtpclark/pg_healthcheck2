from pathlib import Path
from plugins.base import BasePlugin
from .connector import OpenSearchConnector
from .reports.default import get_report_definition as get_opensearch_report_definition

class OpenSearchPlugin(BasePlugin):
    @property
    def technology_name(self):
        return "opensearch"

    def get_connector(self, settings):
        return OpenSearchConnector(settings)

    def get_report_definition(self, report_config_file=None):
        return get_opensearch_report_definition()

    def get_rules_config(self):
        return {} # Placeholder

    def get_template_path(self):
        return Path(__file__).parent.parent / "opensearch" / "templates"
