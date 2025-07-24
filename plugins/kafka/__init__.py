from plugins.base import BasePlugin
from .connector import KafkaConnector

class KafkaPlugin(BasePlugin):
    def __init__(self):
        super().__init__()
        self.technology_name = 'kafka'

    def get_connector(self, settings):
        return KafkaConnector(settings)
