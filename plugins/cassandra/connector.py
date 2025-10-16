from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
import logging

logger = logging.getLogger(__name__)

class CassandraConnector:
    """Handles all direct communication with Cassandra."""

    def __init__(self, settings):
        self.settings = settings
        self.cluster = None
        self.session = None
        self.version_info = {}

    def connect(self):
        """Establishes a connection to the cluster."""
        try:
            contact_points = self.settings.get('hosts', ['localhost'])
            port = self.settings.get('port', 9042)
            
            auth_provider = None
            if self.settings.get('user') and self.settings.get('password'):
                auth_provider = PlainTextAuthProvider(
                    username=self.settings.get('user'),
                    password=self.settings.get('password')
                )
            
            self.cluster = Cluster(
                contact_points=contact_points,
                port=port,
                auth_provider=auth_provider
            )
            
            self.session = self.cluster.connect()
            self.session.row_factory = dict_factory  # Return dicts
            
            # Set keyspace if specified
            keyspace = self.settings.get('keyspace')
            if keyspace:
                self.session.set_keyspace(keyspace)
            
            # Get version info
            self.version_info = self._get_version_info()
            
            print("✅ Successfully connected to Cassandra.")
            print(f"   - Version: {self.version_info.get('version_string', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise ConnectionError(f"Could not connect to Cassandra: {e}")

    def disconnect(self):
        """Closes the connection."""
        if self.cluster:
            try:
                self.cluster.shutdown()
                print("🔌 Disconnected from Cassandra.")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self.cluster = None
                self.session = None

    def close(self):
        """Alias for disconnect() - DB-API 2.0 compatibility."""
        self.disconnect()

    def _get_version_info(self):
        """Fetches version information."""
        try:
            rows = self.session.execute("SELECT release_version FROM system.local")
            version_string = rows[0]['release_version'] if rows else 'Unknown'
            
            # Parse version
            parts = version_string.split('.')
            major = int(parts[0]) if len(parts) > 0 else 0
            
            return {
                'version_string': version_string,
                'major_version': major,
                'is_v3_or_newer': major >= 3,
                'is_v4_or_newer': major >= 4,
            }
        except Exception as e:
            logger.warning(f"Could not fetch version: {e}")
            return {
                'version_string': 'Unknown',
                'major_version': 0,
                'is_v3_or_newer': False,
                'is_v4_or_newer': False,
            }

    def get_db_metadata(self):
        """
        Fetches database metadata.
        
        Returns:
            dict: {'version': str, 'db_name': str}
        """
        try:
            keyspace = self.settings.get('keyspace', 'system')
            return {
                'version': self.version_info.get('version_string', 'N/A'),
                'db_name': keyspace
            }
        except Exception as e:
            logger.warning(f"Could not fetch metadata: {e}")
            return {'version': 'N/A', 'db_name': 'N/A'}

    def execute_query(self, query, params=None, return_raw=False):
        """
        Executes a CQL query and returns formatted results.
        
        Args:
            query: CQL query string
            params: Optional query parameters
            return_raw: If True, returns (formatted, raw_list)
        
        Returns:
            str or tuple: Formatted results
        """
        try:
            # Execute query
            if params:
                rows = self.session.execute(query, params)
            else:
                rows = self.session.execute(query)
            
            # Convert to list of dicts
            raw_results = list(rows)
            
            # Handle empty results
            if not raw_results:
                formatted = "[NOTE]\n====\nNo results returned.\n====\n"
                return (formatted, []) if return_raw else formatted
            
            # Build AsciiDoc table
            columns = list(raw_results[0].keys())
            table = ['|===', '|' + '|'.join(columns)]
            for row in raw_results:
                row_values = [str(row.get(col, '')) for col in columns]
                table.append('|' + '|'.join(row_values))
            table.append('|===')
            formatted = '\n'.join(table)
            
            return (formatted, raw_results) if return_raw else formatted
            
        except Exception as e:
            logger.error(f"CQL query failed: {e}")
            error_msg = f"[ERROR]\n====\nQuery failed: {str(e)}\n====\n"
            return (error_msg, {'error': str(e)}) if return_raw else error_msg