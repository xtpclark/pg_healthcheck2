from plugins.base import BasePlugin
from .connector import OpenSearchConnector

class OpenSearchPlugin(BasePlugin):
    def __init__(self):
        super().__init__()
        self.technology_name = 'opensearch'

    def get_connector(self, settings):
        return OpenSearchConnector(settings)
