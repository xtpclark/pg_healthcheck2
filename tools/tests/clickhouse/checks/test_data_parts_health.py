import unittest
from unittest.mock import MagicMock

from plugins.clickhouse.checks import data_parts_health


class TestDataPartsHealthCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}

    def test_run_data_parts_health(self):
        # Mock the execute_query method to return sample data
        self.connector.execute_query.return_value = (
            "| database | table      | part_name | active | bytes_on_disk | modification_time   |\n"
            "|----------|------------|-----------|--------|---------------|---------------------|\n"
            "| default  | test_table | all_1_1_0 | 0      | 1024          | 2023-01-01 00:00:00 |",
            [{"database": "default", "table": "test_table", "part_name": "all_1_1_0", "active": 0, "bytes_on_disk": 1024, "modification_time": "2023-01-01 00:00:00"}]
        )

        # Run the check
        report, data = data_parts_health.run_data_parts_health(self.connector, self.settings)

        # Assertions
        self.assertIsInstance(report, str)
        self.assertIsInstance(data, dict)
        self.assertIn("data_parts_health", data)
        self.assertIn("Unmerged or outdated data parts", report)

    def test_get_weight(self):
        weight = data_parts_health.get_weight()
        self.assertEqual(weight, 7)


if __name__ == '__main__':
    unittest.main()
