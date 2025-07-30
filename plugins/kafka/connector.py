# Requires: pip install kafka-python
from kafka import KafkaAdminClient

class KafkaConnector:
    """Handles all direct communication with the Kafka cluster."""
    def __init__(self, settings):
        self.settings = settings
        self.admin_client = None
        self.version_info = {}

    def connect(self):
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.settings['bootstrap_servers'],
                security_protocol=self.settings.get('security_protocol', 'PLAINTEXT'),
                # (Add other SASL/SSL configs as needed)
            )
            self.version_info = self._get_version_info()
            print(f"✅ Successfully connected to Kafka cluster.")
            print(f"   - API Version: {self.admin_client.api_version}")
        except Exception as e:
            print(f"❌ Error connecting to Kafka: {e}")
            raise

    def disconnect(self):
        if self.admin_client:
            self.admin_client.close()
            print("🔌 Disconnected from Kafka.")

    def _get_version_info(self):
        # KafkaAdminClient stores this after connecting
        return {'api_version': self.admin_client.api_version}

    def get_db_metadata(self):
        return {'version': str(self.version_info.get('api_version')), 'db_name': 'N/A'}
