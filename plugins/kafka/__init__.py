from pathlib import Path
from plugins.base import BasePlugin
from .connector import KafkaConnector
from .reports.default import get_report_definition as get_kafka_report_definition

class KafkaPlugin(BasePlugin):
    @property
    def technology_name(self):
        return "kafka"

    def get_connector(self, settings):
        return KafkaConnector(settings)

    def get_report_definition(self, report_config_file=None):
        return get_kafka_report_definition()

    def get_rules_config(self):
        return {} # Placeholder

    def get_template_path(self):
        return Path(__file__).parent.parent / "kafka" / "templates"
