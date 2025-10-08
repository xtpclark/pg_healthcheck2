import unittest
from unittest.mock import MagicMock

class TestReplicationStatusCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}

    def test_run_replication_status(self):
        from plugins.clickhouse.checks import replication_status
        
        # Mock the execute_query method to return dummy data
        self.connector.execute_query.return_value = ("Dummy AsciiDoc output", [{"database": "test", "table": "test_table", "active": 1}])
        
        # Run the check
        adoc_output, structured_data = replication_status.run_replication_status(self.connector, self.settings)
        
        # Assertions
        self.assertIsInstance(adoc_output, str)
        self.assertIsInstance(structured_data, dict)
        self.assertIn("replication_status", structured_data)
        self.assertIn("queue_backlog", structured_data)

if __name__ == '__main__':
    unittest.main()