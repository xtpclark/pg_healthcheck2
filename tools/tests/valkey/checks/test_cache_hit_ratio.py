# -*- coding: utf-8 -*-
# test_cache_hit_ratio.py: Unit tests for cache hit/miss ratio check

import unittest
from unittest.mock import MagicMock

from plugins.valkey.checks import cache_hit_ratio


class TestCacheHitRatioCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}

    def test_run_cache_hit_ratio(self):
        # Mock the connector response for INFO STATS
        mock_response = {
            'keyspace_hits': '1000',
            'keyspace_misses': '500'
        }
        self.connector.execute_query.return_value = ('mocked_table', mock_response)

        # Run the check
        report, data = cache_hit_ratio.run_cache_hit_ratio(self.connector, self.settings)

        # Assertions
        self.assertIsInstance(report, str)
        self.assertIsInstance(data, dict)
        self.assertIn('cache_hit_ratio', data)
        self.assertEqual(data['cache_hit_ratio']['status'], 'success')

    def test_run_cache_hit_ratio_error(self):
        # Simulate an exception
        self.connector.execute_query.side_effect = Exception('Connection error')

        # Run the check
        report, data = cache_hit_ratio.run_cache_hit_ratio(self.connector, self.settings)

        # Assertions
        self.assertIsInstance(report, str)
        self.assertIsInstance(data, dict)
        self.assertIn('cache_hit_ratio', data)
        self.assertEqual(data['cache_hit_ratio']['status'], 'error')


if __name__ == '__main__':
    unittest.main()
