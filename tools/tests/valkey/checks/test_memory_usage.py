import unittest
from unittest.mock import MagicMock

class TestMemoryUsageCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}
        self.connector.execute_query.return_value = ('Formatted Table', {
            'used_memory': 1000000,
            'used_memory_human': '1M',
            'max_memory': 2000000,
            'max_memory_human': '2M',
            'mem_fragmentation_ratio': 1.2
        })

    def test_run_memory_usage(self):
        from plugins.valkey.checks.memory_usage import run_memory_usage
        report, data = run_memory_usage(self.connector, self.settings)
        self.assertIsInstance(report, str)
        self.assertIsInstance(data, dict)
        self.assertIn('memory_usage', data)
        self.assertIn('fragmentation_ratio', data)

if __name__ == '__main__':
    unittest.main()