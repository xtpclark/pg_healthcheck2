# -*- coding: utf-8 -*-
# Unit Test for High Memory Usage Queries Check

import unittest
from unittest.mock import MagicMock

from plugins.clickhouse.checks import high_memory_queries


class TestHighMemoryQueriesCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}

    def test_run_high_memory_queries_check(self):
        # Mock the execute_query method
        self.connector.execute_query.return_value = ('formatted_table', [{'query_id': 'test', 'memory_usage': 2000000000}])

        # Run the check
        result, data = high_memory_queries.run_high_memory_queries_check(self.connector, self.settings)

        # Assertions
        self.assertIsInstance(result, str)
        self.assertIsInstance(data, dict)
        self.assertIn('high_memory_queries', data)
        self.assertEqual(data['high_memory_queries']['status'], 'success')

    def test_weight(self):
        weight = high_memory_queries.get_weight()
        self.assertEqual(weight, 8)


if __name__ == '__main__':
    unittest.main()
