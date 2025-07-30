# Requires: pip install valkey
import valkey

class ValkeyConnector:
    def __init__(self, settings):
        self.settings = settings
        self.client = None
        self.version_info = {}

    def connect(self):
        try:
            self.client = valkey.Valkey(
                host=self.settings['host'],
                port=self.settings.get('port', 6379),
                username=self.settings.get('user'),
                password=self.settings.get('password'),
                db=self.settings.get('db', 0),
                decode_responses=True
            )
            self.client.ping()
            self.version_info = self.client.info()
            print(f"✅ Successfully connected to Valkey.")
            print(f"   - Version: {self.version_info.get('valkey_version', 'Unknown')}")
        except Exception as e:
            print(f"❌ Error connecting to Valkey: {e}")
            raise

    def disconnect(self):
        if self.client:
            self.client.close()
            print("🔌 Disconnected from Valkey.")

    def get_db_metadata(self):
        return {'version': self.version_info.get('valkey_version'), 'db_name': str(self.settings.get('db'))}
