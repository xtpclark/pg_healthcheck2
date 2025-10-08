import clickhouse_driver

class ClickhouseConnector:
    """Handles all direct communication with Clickhouse."""

    def __init__(self, settings):
        self.settings = settings
        self.conn = None

    def connect(self):
        """Establishes a connection to the database."""
        self.conn = clickhouse_driver.Client(
            host=self.settings['host'],
            port=self.settings.get('port', 9000),
            user=self.settings.get('user', 'default'),
            password=self.settings.get('password', ''),
            database=self.settings.get('database', 'default')
        )

    def disconnect(self):
        if self.conn:
            self.conn.disconnect()

    def execute_query(self, query, params=None, return_raw=False):
        """Executes a query and returns formatted and raw results."""
        raw_results = self.conn.execute(query, params or {})
        columns = self.conn.last_query.profile_info().get('columns', [])
        if not columns:
            columns = [f"column_{i}" for i in range(len(raw_results[0]))] if raw_results else []
        formatted_results = [dict(zip(columns, [str(item) for item in row])) for row in raw_results]

        if not raw_results:
            return ("[NOTE]\n====\nNo results returned.\n====\n", []) if return_raw else "[NOTE]\n====\nNo results returned.\n====\n"

        # Basic AsciiDoc table formatting
        table = ['|===', '|' + '|'.join(columns)]
        for row in formatted_results:
            table.append('|' + '|'.join(row.values()))
        table.append('|===')
        formatted = '\n'.join(table)

        return (formatted, formatted_results) if return_raw else formatted
