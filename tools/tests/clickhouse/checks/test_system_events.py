import unittest
from unittest.mock import MagicMock

from plugins.clickhouse.checks import system_events


class TestSystemEventsCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}

    def test_run_system_events_check(self):
        # Mock the execute_query method to return sample data
        self.connector.execute_query.return_value = (
            "| event          | value | description          |\n" +
            "|---------------|-------|----------------------|\n" +
            "| ErrorDetected | 1     | An error occurred    |",
            [{"event": "ErrorDetected", "value": 1, "description": "An error occurred"}]
        )

        # Run the check
        report, data = system_events.run_system_events_check(self.connector, self.settings)

        # Assertions
        self.assertIsInstance(report, str)
        self.assertIsInstance(data, dict)
        self.assertIn("system_events_analysis", data)
        self.assertEqual(data["system_events_analysis"]["status"], "success")

    def test_run_system_events_check_error(self):
        # Simulate an error during query execution
        self.connector.execute_query.side_effect = Exception("Query failed")

        # Run the check
        report, data = system_events.run_system_events_check(self.connector, self.settings)

        # Assertions
        self.assertIsInstance(report, str)
        self.assertIsInstance(data, dict)
        self.assertIn("system_events_analysis", data)
        self.assertEqual(data["system_events_analysis"]["status"], "error")


if __name__ == '__main__':
    unittest.main()
