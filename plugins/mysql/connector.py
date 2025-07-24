# You will need to install a mysql driver, e.g., pip install mysql-connector-python
import mysql.connector

class MySQLConnector:
    """Handles all direct communication with the MySQL database."""

    def __init__(self, settings):
        self.settings = settings
        self.conn = None
        self.cursor = None
        self.version_info = {}

    def connect(self):
        """Establishes a connection to the database and fetches version info."""
        try:
            self.conn = mysql.connector.connect(
                host=self.settings['host'],
                port=self.settings['port'],
                database=self.settings['database'],
                user=self.settings['user'],
                password=self.settings['password']
            )
            self.cursor = self.conn.cursor(dictionary=True) # Use dictionary cursor for easy row access
            self.version_info = self._get_version_info()
            
            print("✅ Successfully connected to MySQL.")
            print(f"   - Version: {self.version_info.get('version_string', 'Unknown')}")

        except mysql.connector.Error as e:
            print(f"❌ Error connecting to MySQL: {e}")
            raise

    def disconnect(self):
        """Closes the database connection."""
        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
            print("🔌 Disconnected from MySQL.")

    def _get_version_info(self):
        """Private method to get MySQL version information."""
        try:
            self.cursor.execute("SELECT VERSION();")
            version_string = self.cursor.fetchone()['VERSION()']
            # Basic version parsing, can be enhanced
            major_version = int(version_string.split('.')[0])
            
            return {
                'version_string': version_string,
                'major_version': major_version
                # Add more flags as needed, e.g., is_mysql8_or_newer
            }
        except Exception:
            return {}

    def execute_query(self, query, params=None, return_raw=False):
        """Executes a query and returns formatted and raw results."""
        # This method needs to be implemented to format results into AsciiDoc tables,
        # similar to the PostgresConnector. For now, it's a simple pass-through.
        try:
            self.cursor.execute(query, params or ())
            if self.cursor.description is None:
                return ("", []) if return_raw else ""

            columns = self.cursor.column_names
            raw_results = self.cursor.fetchall()

            # (Add formatting logic here to build AsciiDoc tables)
            formatted_result = "Table formatting not yet implemented for MySQL."
            
            return (formatted_result, raw_results) if return_raw else formatted_result
        except mysql.connector.Error as e:
            error_str = f"[ERROR]\n====\nQuery failed: {e}\n====\n"
            return (error_str, {"error": str(e)}) if return_raw else error_str

    def get_db_metadata(self):
        """Fetches basic metadata for the AI prompt."""
        return {
            'version': self.version_info.get('version_string', 'N/A'),
            'db_name': self.settings.get('database', 'N/A')
        }

    def _get_version_info(self):
        """Private method to get MySQL version information."""
        try:
            self.cursor.execute("SELECT @@version AS version, @@version_comment AS source;")
            result = self.cursor.fetchone()
            version_string = result['version'] if result else 'Unknown'
            
            # --- NEW: More detailed version parsing ---
            major_version = 0
            is_mariadb = 'mariadb' in result.get('source', '').lower()
            
            if version_string:
                major_version = int(version_string.split('.')[0])

            return {
                'version_string': version_string,
                'major_version': major_version,
                'is_mariadb': is_mariadb,
                'is_mysql8_or_newer': not is_mariadb and major_version >= 8,
                'is_mysql5_7': not is_mariadb and major_version == 5 and '5.7' in version_string
                # Add other specific version flags as needed
            }
        except Exception as e:
            print(f"Warning: Could not parse MySQL version string: {e}")
            return {}
