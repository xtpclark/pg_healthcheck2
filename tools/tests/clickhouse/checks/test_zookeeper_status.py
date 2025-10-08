# -*- coding: utf-8 -*-
# Copyright (c) 2023-2024, HealthCheck2 Team
# License: See LICENSE file

import unittest
from unittest.mock import MagicMock

from plugins.clickhouse.checks import zookeeper_status


class TestZooKeeperStatusCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}

    def test_run_zookeeper_status_check(self):
        # Mock the connector's execute_query method
        self.connector.execute_query.return_value = (
            "=== Mock ZooKeeper Status\n| host | port | connected |\n| mock | 2181 | 1 |",
            [{'host': 'mock', 'port': 2181, 'connected': 1}]
        )

        # Run the check
        report, data = zookeeper_status.run_zookeeper_status_check(self.connector, self.settings)

        # Assertions
        self.assertIsInstance(report, str)
        self.assertIsInstance(data, dict)
        self.assertIn('zookeeper_status', data)
        self.assertIn('ZooKeeper Connection Status Check', report)

    def test_get_weight(self):
        weight = zookeeper_status.get_weight()
        self.assertIsInstance(weight, int)
        self.assertTrue(1 <= weight <= 10)


if __name__ == '__main__':
    unittest.main()
