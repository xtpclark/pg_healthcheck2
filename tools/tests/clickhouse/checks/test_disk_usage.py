import unittest
from unittest.mock import MagicMock

from plugins.clickhouse.checks import disk_usage


class TestDiskUsageCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}

    def test_run_disk_usage(self):
        # Mock the execute_query method
        self.connector.execute_query.return_value = (
            'Formatted Table', [{'name': 'disk1', 'free_space': 100, 'total_space': 1000, 'free_space_percent': 10.0}]
        )

        # Run the check
        report, data = disk_usage.run_disk_usage(self.connector, self.settings)

        # Assertions
        self.assertIsInstance(report, str)
        self.assertIsInstance(data, dict)
        self.assertIn('disk_usage', data)
        self.assertEqual(data['disk_usage']['status'], 'success')

    def test_get_weight(self):
        weight = disk_usage.get_weight()
        self.assertIsInstance(weight, int)
        self.assertTrue(1 <= weight <= 10)


if __name__ == '__main__':
    unittest.main()
