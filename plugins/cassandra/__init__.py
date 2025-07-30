from pathlib import Path
from plugins.base import BasePlugin
from .connector import CassandraConnector
from .reports.default import get_report_definition as get_cassandra_report_definition
# from .rules.analysis_rules import METRIC_ANALYSIS_CONFIG as CASSANDRA_ANALYSIS_RULES

class CassandraPlugin(BasePlugin):
    @property
    def technology_name(self):
        return "cassandra"

    def get_connector(self, settings):
        return CassandraConnector(settings)

    def get_report_definition(self, report_config_file=None):
        return get_cassandra_report_definition()

    def get_rules_config(self):
        # return CASSANDRA_ANALYSIS_RULES
        return {} # Placeholder

    def get_template_path(self):
        # Path to prompt templates
        return Path(__file__).parent.parent / "cassandra" / "templates"
