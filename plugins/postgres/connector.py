import psycopg2
from plugins.base import BasePlugin # We will use the base for type hinting later

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
            raise # Re-raise the exception to be handled by the core engine

    def disconnect(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("🔌 Disconnected from PostgreSQL.")

    def execute_query(self, query, params=None, is_check=False, return_raw=False):
        """Executes a query and returns formatted and raw results."""
        try:
            self.cursor.execute(query, params)
            if is_check:
                result = self.cursor.fetchone()[0] if self.cursor.rowcount > 0 else ""
                return (str(result), result) if return_raw else str(result)
            
            if self.cursor.description is None: # For queries that don't return rows (e.g., SET)
                return ("", []) if return_raw else ""

            columns = [desc[0] for desc in self.cursor.description]
            results = self.cursor.fetchall()
            raw_results = [dict(zip(columns, row)) for row in results]

            if not results:
                formatted = "[NOTE]\\n====\\nNo results returned.\\n====\\n"
                return (formatted, []) if return_raw else formatted

            table = ['|===', '|' + '|'.join(columns)]
            for row in results:
                # Sanitize cell content to prevent breaking AsciiDoc tables
                sanitized_row = [str(v).replace('|', '\\|') if v is not None else '' for v in row]
                table.append('|' + '|'.join(sanitized_row))
            table.append('|===')
            formatted = '\\n'.join(table)
            
            return (formatted, raw_results) if return_raw else formatted
        except psycopg2.Error as e:
            if self.conn:
                self.conn.rollback()
            error_str = f"[ERROR]\\n====\\nQuery failed: {e}\\n====\\n"
            return (error_str, {"error": str(e), "query": query}) if return_raw else error_str
