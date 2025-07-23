import psycopg2
import subprocess
from plugins.base import BasePlugin

class PostgresConnector:
    """Handles all direct communication with the PostgreSQL database."""

    def __init__(self, settings):
        self.settings = settings
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establishes a connection to the database."""
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
            print("✅ Successfully connected to PostgreSQL.")
        except psycopg2.Error as e:
            print(f"❌ Error connecting to PostgreSQL: {e}")
            raise

    def disconnect(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("🔌 Disconnected from PostgreSQL.")

    def get_db_metadata(self):
        """
        Fetches basic metadata like version and database name.
        This method is required for the core engine to remain agnostic.
        """
        try:
            # Fetch version string
            version_query = "SELECT version();"
            _, raw_version = self.execute_query(version_query, return_raw=True)
            version_str = raw_version[0]['version'] if raw_version else 'N/A'

            # Fetch database name
            dbname_query = "SELECT current_database();"
            _, raw_dbname = self.execute_query(dbname_query, return_raw=True)
            db_name = raw_dbname[0]['current_database'] if raw_dbname else 'N/A'

            return {
                'version': version_str,
                'db_name': db_name
            }
        except Exception as e:
            print(f"Warning: Could not fetch database metadata: {e}")
            return {
                'version': 'N/A',
                'db_name': 'N/A'
            }

    def execute_query(self, query, params=None, is_check=False, return_raw=False):
        """Executes a query and returns formatted and raw results."""
        try:
            self.cursor.execute(query, params)
            if is_check:
                result = self.cursor.fetchone()[0] if self.cursor.rowcount > 0 else ""
                return (str(result), result) if return_raw else str(result)
            
            if self.cursor.description is None:
                return ("", []) if return_raw else ""

            columns = [desc[0] for desc in self.cursor.description]
            results = self.cursor.fetchall()
            raw_results = [dict(zip(columns, row)) for row in results]

            if not results:
                return "[NOTE]\n====\nNo results returned.\n====\n", [] if return_raw else ""

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
