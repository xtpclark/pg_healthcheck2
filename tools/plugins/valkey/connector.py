import redis

class ValkeyConnector:
    """Handles all direct communication with Valkey."""

    def __init__(self, settings):
        self.settings = settings
        self.conn = None

    def connect(self):
        """Establishes a connection to the database."""
        self.conn = redis.Redis(
            host=self.settings['host'],
            port=self.settings['port'],
            password=self.settings.get('password'),
            decode_responses=True  # Important for getting strings
        )
        self.conn.ping()

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def execute_query(self, query, params=None, return_raw=False):
        """Executes a query and returns formatted and raw results."""
        raw_results = {}
        if query.strip().upper() == "INFO MEMORY":
            raw_results = self.conn.info('memory')
        else:
            # Placeholder for other Valkey commands
            raw_results = {"error": "Command not implemented in boilerplate"}

        # Format the dictionary into a two-column AsciiDoc table
        columns = ['Metric', 'Value']
        table = ['|===', '|' + '|'.join(columns)]
        for key, value in raw_results.items():
            table.append(f'|{key}|{value}')
        table.append('|===')
        formatted = '\n'.join(table)
        
        return (formatted, raw_results) if return_raw else formatted
