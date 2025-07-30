# Requires: pip install cassandra-driver
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class CassandraConnector:
    """Handles all direct communication with the Cassandra cluster."""
    def __init__(self, settings):
        self.settings = settings
        self.cluster = None
        self.session = None
        self.version_info = {}

    def connect(self):
        try:
            auth_provider = PlainTextAuthProvider(
                username=self.settings['user'],
                password=self.settings['password']
            )
            self.cluster = Cluster(
                self.settings['contact_points'],
                port=self.settings.get('port', 9042),
                auth_provider=auth_provider
            )
            self.session = self.cluster.connect()
            self.version_info = self._get_version_info()
            print(f"✅ Successfully connected to Cassandra cluster.")
            print(f"   - Release Version: {self.version_info.get('release_version', 'Unknown')}")
        except Exception as e:
            print(f"❌ Error connecting to Cassandra: {e}")
            raise

    def disconnect(self):
        if self.cluster:
            self.cluster.shutdown()
            print("🔌 Disconnected from Cassandra.")

    def _get_version_info(self):
        try:
            row = self.session.execute("SELECT release_version FROM system.local").one()
            return {'release_version': row.release_version}
        except Exception:
            return {}

    def execute_query(self, query, params=None, return_raw=False):
        """Executes a CQL query and returns formatted and raw results."""
        try:
            rows = self.session.execute(query, params or ())
            raw_results = [row._asdict() for row in rows]

            if not raw_results:
                return "[NOTE]\n====\nNo results returned.\n====\n", [] if return_raw else ""

            columns = raw_results[0].keys()
            table = ['|===', '|' + '|'.join(columns)]
            for row_dict in raw_results:
                sanitized_row = [str(v).replace('|', '\\|') if v is not None else '' for v in row_dict.values()]
                table.append('|' + '|'.join(sanitized_row))
            table.append('|===')
            formatted_result = '\n'.join(table)
            
            return (formatted_result, raw_results) if return_raw else formatted_result
        except Exception as e:
            error_str = f"[ERROR]\n====\nQuery failed: {e}\n====\n"
            return (error_str, {"error": str(e)}) if return_raw else error_str

    def get_db_metadata(self):
        return {'version': self.version_info.get('release_version', 'N/A'), 'db_name': 'N/A'}
