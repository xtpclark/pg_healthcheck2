# Requires: pip install clickhouse-connect
import clickhouse_connect

class ClickHouseConnector:
    def __init__(self, settings):
        self.settings = settings
        self.client = None
        self.version_info = {}

    def connect(self):
        try:
            self.client = clickhouse_connect.get_client(
                host=self.settings['host'],
                port=self.settings.get('port', 8123),
                username=self.settings['user'],
                password=self.settings['password']
            )
            self.version_info = self._get_version_info()
            print(f"✅ Successfully connected to ClickHouse.")
            print(f"   - Version: {self.version_info.get('version', 'Unknown')}")
        except Exception as e:
            print(f"❌ Error connecting to ClickHouse: {e}")
            raise

    def disconnect(self):
        if self.client:
            self.client.close()
            print("🔌 Disconnected from ClickHouse.")

    def _get_version_info(self):
        try:
            row = self.client.query("SELECT version() as version").first_row
            return {'version': row[0]}
        except Exception:
            return {}

    def execute_query(self, query, params=None, return_raw=False):
        """Executes a query and returns formatted and raw results."""
        try:
            result = self.client.query(query, parameters=params)
            raw_results = result.result_rows # Returns a list of tuples
            columns = result.column_names

            if not raw_results:
                return "[NOTE]\n====\nNo results returned.\n====\n", [] if return_raw else ""

            # --- AsciiDoc Formatting Logic ---
            table = ['|===', '|' + '|'.join(columns)]
            for row_tuple in raw_results:
                sanitized_row = [str(v).replace('|', '\\|') if v is not None else '' for v in row_tuple]
                table.append('|' + '|'.join(sanitized_row))
            table.append('|===')
            formatted_result = '\n'.join(table)
            
            # Convert list of tuples to list of dicts for structured data
            structured_results = [dict(zip(columns, row)) for row in raw_results]
            
            return (formatted_result, structured_results) if return_raw else formatted_result
        except Exception as e:
            error_str = f"[ERROR]\n====\nQuery failed: {e}\n====\n"
            return (error_str, {"error": str(e)}) if return_raw else error_str

    def get_db_metadata(self):
        return {'version': self.version_info.get('version'), 'db_name': 'N/A'}
