# -*- coding: utf-8 -*-
# Unit tests for system_resource_usage check

import unittest
from unittest.mock import MagicMock

from plugins.clickhouse.checks import system_resource_usage


class TestSystemResourceUsageCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}
        self.connector.execute_query.return_value = (
            "Formatted table output",
            [
                {"metric": "CPUUsage", "value": 75, "description": "CPU usage percentage"},
                {"metric": "MemoryUsage", "value": 60, "description": "Memory usage percentage"},
                {"metric": "DiskIO", "value": 50, "description": "Disk I/O usage percentage"}
            ]
        )

    def test_run_system_resource_usage(self):
        result, data = system_resource_usage.run_system_resource_usage(self.connector, self.settings)
        self.assertIsInstance(result, str)
        self.assertIsInstance(data, dict)
        self.assertIn("system_resources", data)
        self.assertEqual(data["system_resources"]["status"], "success")

    def test_get_weight(self):
        weight = system_resource_usage.get_weight()
        self.assertIsInstance(weight, int)
        self.assertTrue(1 <= weight <= 10)


if __name__ == '__main__':
    unittest.main()
