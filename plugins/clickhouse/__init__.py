from plugins.base import BasePlugin
from .connector import ClickHouseConnector

class ClickHousePlugin(BasePlugin):
    def __init__(self):
        super().__init__()
        self.technology_name = 'clickhouse'

    def get_connector(self, settings):
        return ClickHouseConnector(settings)
