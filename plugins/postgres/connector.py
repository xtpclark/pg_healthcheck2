import psycopg2
import subprocess
from plugins.base import BasePlugin

class PostgresConnector:
    """Handles all direct communication with the PostgreSQL database."""

    def __init__(self, settings):
        self.settings = settings
        self.conn = None
        self.cursor = None
        self.version_info = {}
        self.has_pgstat = False
        # --- NEW ATTRIBUTES ---
        # Flag for PG13-16 style I/O columns (e.g., blk_read_time)
        self.has_pgstat_legacy_io_time = False 
        # Flag for PG17+ style I/O columns (e.g., shared_blk_read_time)
        self.has_pgstat_new_io_time = False 

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
            
            # --- MODIFIED: Consolidated capability checks ---
            self._check_pg_stat_capabilities()
            
            # --- MODIFIED: Enhanced connection status message ---
            print("✅ Successfully connected to PostgreSQL.")
            print(f"   - Version: {self.version_info.get('version_string', 'Unknown')}")
            print(f"   - pg_stat_statements: {'Enabled' if self.has_pgstat else 'Not Found'}")
            if self.has_pgstat:
                io_status = "Not Available"
                if self.has_pgstat_new_io_time:
                    io_status = "Available (PG17+ Style)"
                elif self.has_pgstat_legacy_io_time:
                    io_status = "Available (Legacy Style)"
                print(f"   - I/O Timings in pg_stat_statements: {io_status}")

        except psycopg2.Error as e:
            print(f"❌ Error connecting to PostgreSQL: {e}")
            raise

    # --- REPLACED _check_pg_stat_statements with a more comprehensive method ---
    def _check_pg_stat_capabilities(self):
        """Checks for the existence and capabilities of the pg_stat_statements extension."""
        try:
            # First, check if the extension exists at all
            _, ext_exists = self.execute_query("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements');", is_check=True, return_raw=True)
            self.has_pgstat = (str(ext_exists).lower() in ['t', 'true'])

            if self.has_pgstat:
                # If it exists, check for PG17+ style columns FIRST for forward-compatibility
                query_new = "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_stat_statements' AND column_name = 'shared_blk_read_time');"
                _, col_exists_new = self.execute_query(query_new, is_check=True, return_raw=True)
                self.has_pgstat_new_io_time = (str(col_exists_new).lower() in ['t', 'true'])

                # If new columns don't exist, check for legacy columns
                if not self.has_pgstat_new_io_time:
                    query_legacy = "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_stat_statements' AND column_name = 'blk_read_time');"
                    _, col_exists_legacy = self.execute_query(query_legacy, is_check=True, return_raw=True)
                    self.has_pgstat_legacy_io_time = (str(col_exists_legacy).lower() in ['t', 'true'])

        except Exception as e:
            print(f"Warning: Could not check for pg_stat_statements capabilities: {e}")
            self.has_pgstat = False
            self.has_pgstat_legacy_io_time = False
            self.has_pgstat_new_io_time = False

    def disconnect(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("🔌 Disconnected from PostgreSQL.")

    def _get_version_info(self):
        """
        Private method to get PostgreSQL version information.
        """
        try:
            self.cursor.execute("SELECT current_setting('server_version_num');")
            version_num = int(self.cursor.fetchone()[0].strip())
            
            self.cursor.execute("SELECT current_setting('server_version');")
            version_string = self.cursor.fetchone()[0].strip()
            
            major_version = version_num // 10000
            
            # --- CORRECTED: Added all necessary version flags ---
            return {
                'version_num': version_num,
                'version_string': version_string,
                'major_version': major_version,
                'is_pg10_or_newer': major_version >= 10,
                'is_pg11_or_newer': major_version >= 11,
                'is_pg12_or_newer': major_version >= 12,
                'is_pg13_or_newer': major_version >= 13,
                'is_pg14_or_newer': major_version >= 14,
                'is_pg15_or_newer': major_version >= 15,
                'is_pg16_or_newer': major_version >= 16,
                'is_pg17_or_newer': major_version >= 17,
                'is_pg18_or_newer': major_version >= 18
            }
        except Exception:
            # Fallback if all methods fail
            return {
                'version_num': 0, 'version_string': 'unknown', 'major_version': 0,
                'is_pg10_or_newer': False, 'is_pg11_or_newer': False,
                'is_pg12_or_newer': False, 'is_pg13_or_newer': False,
                'is_pg14_or_newer': False, 'is_pg15_or_newer': False,
                'is_pg16_or_newer': False, 'is_pg17_or_newer': False,
                'is_pg18_or_newer': False
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

    def has_select_privilege(self, view_name):
        """Checks if the current user has SELECT privilege on a given view/table."""
        try:
            # Use has_table_privilege for a direct boolean check
            query = f"SELECT has_table_privilege(current_user, '{view_name}', 'SELECT');"
            _, has_priv = self.execute_query(query, is_check=True, return_raw=True)
            return (str(has_priv).lower() in ['t', 'true'])
        except Exception as e:
            print(f"Warning: Could not check privilege for {view_name}: {e}")
            return False
