import psycopg2
import subprocess
from plugins.base import BasePlugin

class PostgresConnector:
    """Handles all direct communication with the PostgreSQL database."""

    def __init__(self, settings):
        self.settings = settings
        self.conn = None
        self.cursor = None
        self.version_info = {}  # Attribute to store version details
        self.has_pgstat = False  # Attribute to store pg_stat_statements availability

    def connect(self):
        """Establishes a connection to the database and fetches version info."""
        try:
            timeout = self.settings.get('statement_timeout', 30000)
            self.conn = psycopg2.connect(
                host=self.settings['host'],
                port=self.settings['port'],
                dbname=self.settings['database'],
                user=self.settings['user'],
                password=self.settings['password'],
                options=f"-c statement_timeout={timeout}"
            )
            self.conn.autocommit = self.settings.get('autocommit', True)
            self.cursor = self.conn.cursor()
            
            # Get and store version info immediately after connecting
            self.version_info = self._get_version_info()
            
            # Check for pg_stat_statements after establishing the connection
            self._check_pg_stat_statements()
            
            print("✅ Successfully connected to PostgreSQL.")
            print(f"   - Version: {self.version_info.get('version_string', 'Unknown')}")
            print(f"   - pg_stat_statements enabled: {self.has_pgstat}")

        except psycopg2.Error as e:
            print(f"❌ Error connecting to PostgreSQL: {e}")
            raise

    def _check_pg_stat_statements(self):
        """Checks if the pg_stat_statements extension is available."""
        try:
            query = "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements');"
            _, ext_exists = self.execute_query(query, is_check=True, return_raw=True)
            self.has_pgstat = (str(ext_exists).lower() == 't' or str(ext_exists).lower() == 'true')
        except Exception as e:
            print(f"Warning: Could not check for pg_stat_statements extension: {e}")
            self.has_pgstat = False

    def disconnect(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("🔌 Disconnected from PostgreSQL.")

    def _get_version_info(self):
        """
        Private method to get PostgreSQL version information.
        This logic is moved from the compatibility module.
        """
        try:
            # Use `current_setting` for a cleaner output
            version_query = "SELECT current_setting('server_version_num');"
            self.cursor.execute(version_query)
            # Use .strip() just in case the raw output has whitespace
            version_num = int(self.cursor.fetchone()[0].strip())
            
            version_string_query = "SELECT current_setting('server_version');"
            self.cursor.execute(version_string_query)
            version_string = self.cursor.fetchone()[0].strip()
            
            major_version = version_num // 10000
            
            return {
                'version_num': version_num,
                'version_string': version_string,
                'major_version': major_version,
                'is_pg13_or_newer': major_version >= 13,
                'is_pg14_or_newer': major_version >= 14,
                'is_pg15_or_newer': major_version >= 15,
                'is_pg16_or_newer': major_version >= 16,
                'is_pg17_or_newer': major_version >= 17,
                'is_pg18_or_newer': major_version >= 18,
                'is_pg13': major_version == 13,
                'is_pg14': major_version == 14,
                'is_pg15': major_version == 15,
                'is_pg16': major_version == 16,
                'is_pg17': major_version == 17,
                'is_pg18': major_version >= 18
            }
        except Exception:
            # Fallback if all methods fail
            return {
                'version_num': 0, 'version_string': 'unknown', 'major_version': 0,
                'is_pg13_or_newer': False, 'is_pg14_or_newer': False,
                'is_pg15_or_newer': False, 'is_pg16_or_newer': False,
                'is_pg17_or_newer': False, 'is_pg18_or_newer': False,
                'is_pg13': False, 'is_pg14': False, 'is_pg15': False,
                'is_pg16': False, 'is_pg17': False, 'is_pg18': False
            }

    def get_db_metadata(self):
        """Fetches basic metadata like version and database name."""
        try:
            # Now uses the stored version info
            dbname_query = "SELECT current_database();"
            self.cursor.execute(dbname_query)
            db_name = self.cursor.fetchone()[0].strip()

            return {
                'version': self.version_info.get('version_string', 'N/A'),
                'db_name': db_name
            }
        except Exception as e:
            print(f"Warning: Could not fetch database metadata: {e}")
            return {
                'version': self.version_info.get('version_string', 'N/A'),
                'db_name': 'N/A'
            }

    def execute_query(self, query, params=None, is_check=False, return_raw=False):
        """Executes a query and returns formatted and raw results."""
        try:
            if not self.cursor or self.cursor.closed:
                self.cursor = self.conn.cursor()

            self.cursor.execute(query, params)
            
            if is_check:
                # For checks returning a single value, fetchone is appropriate
                result = self.cursor.fetchone()[0] if self.cursor.rowcount > 0 else ""
                # Return the raw Python type alongside its string representation
                return (str(result), result) if return_raw else str(result)
            
            if self.cursor.description is None:
                # This can happen for statements that don't return rows (e.g., SET)
                return ("", []) if return_raw else ""

            columns = [desc[0] for desc in self.cursor.description]
            results = self.cursor.fetchall()
            raw_results = [dict(zip(columns, row)) for row in results]

            if not results:
                return "[NOTE]\n====\nNo results returned.\n====\n", [] if return_raw else ""

            # Formatting logic for ASCII tables
            table = ['|===', '|' + '|'.join(columns)]
            for row in results:
                sanitized_row = [str(v).replace('|', '\\|') if v is not None else '' for v in row]
                table.append('|' + '|'.join(sanitized_row))
            table.append('|===')
            formatted = '\n'.join(table)
            
            return (formatted, raw_results) if return_raw else formatted
        except psycopg2.Error as e:
            if self.conn:
                self.conn.rollback()
            error_str = f"[ERROR]\n====\nQuery failed: {e}\n====\n"
            return (error_str, {"error": str(e), "query": query}) if return_raw else error_str
