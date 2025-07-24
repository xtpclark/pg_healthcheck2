from plugins.base import BasePlugin
from .connector import ValkeyConnector

class ValkeyPlugin(BasePlugin):
    def __init__(self):
        super().__init__()
        self.technology_name = 'valkey'

    def get_connector(self, settings):
        return ValkeyConnector(settings)
