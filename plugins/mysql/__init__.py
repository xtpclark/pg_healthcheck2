from plugins.base import BasePlugin
from .connector import MySQLConnector
from .reports.default import get_report_definition as get_mysql_report_definition
from .rules.analysis_rules import METRIC_ANALYSIS_CONFIG as MYSQL_ANALYSIS_RULES

class MySQLPlugin(BasePlugin):
    def __init__(self):
        super().__init__()
        self.technology_name = 'mysql'

    def get_connector(self, settings):
        return MySQLConnector(settings)

    def get_report_definition(self, report_config_file=None):
        return get_mysql_report_definition()

    def get_rules_config(self):
        return MYSQL_ANALYSIS_RULES
