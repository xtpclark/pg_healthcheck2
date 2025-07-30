# Requires: pip install opensearch-py
from opensearchpy import OpenSearch

class OpenSearchConnector:
    def __init__(self, settings):
        self.settings = settings
        self.client = None
        self.version_info = {}

    def connect(self):
        try:
            self.client = OpenSearch(
                hosts=[{'host': self.settings['host'], 'port': self.settings.get('port', 9200)}],
                http_auth=(self.settings['user'], self.settings['password']),
                use_ssl=self.settings.get('use_ssl', True),
                verify_certs=self.settings.get('verify_certs', True),
                ssl_assert_hostname=self.settings.get('ssl_assert_hostname', True)
            )
            self.version_info = self.client.info()['version']
            print(f"✅ Successfully connected to OpenSearch cluster.")
            print(f"   - Version: {self.version_info.get('number', 'Unknown')}")
        except Exception as e:
            print(f"❌ Error connecting to OpenSearch: {e}")
            raise

    def disconnect(self):
        if self.client:
            self.client.close()
            print("🔌 Disconnected from OpenSearch.")

    def get_db_metadata(self):
        return {'version': self.version_info.get('number'), 'db_name': 'N/A'}
