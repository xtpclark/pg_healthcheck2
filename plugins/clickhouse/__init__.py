from pathlib import Path
from plugins.base import BasePlugin
from .connector import ClickHouseConnector
from .reports.default import get_report_definition as get_clickhouse_report_definition

class ClickHousePlugin(BasePlugin):
    @property
    def technology_name(self):
        return "clickhouse"

    def get_connector(self, settings):
        return ClickHouseConnector(settings)

    def get_report_definition(self, report_config_file=None):
        return get_clickhouse_report_definition()

    def get_rules_config(self):
        return {} # Placeholder

    def get_template_path(self):
        return Path(__file__).parent.parent / "clickhouse" / "templates"
