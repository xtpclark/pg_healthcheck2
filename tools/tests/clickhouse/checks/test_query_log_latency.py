import unittest
from unittest.mock import MagicMock

class TestQueryLogLatencyCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {'high_latency_threshold_ms': 1000}

    def test_run_query_log_latency_check(self):
        from plugins.clickhouse.checks.query_log_latency import run_query_log_latency_check
        result, data = run_query_log_latency_check(self.connector, self.settings)
        self.assertIsInstance(result, str)
        self.assertIsInstance(data, dict)
        self.assertIn('query_log_analysis', data)
        self.assertIn('recommendations', data)

if __name__ == '__main__':
    unittest.main()
