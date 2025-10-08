import unittest
from unittest.mock import MagicMock

class TestKeyspaceStatsCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}
        self.connector.execute_command.return_value = 'db0:keys=500,expires=100,avg_ttl=3600\ndb1:keys=200,expires=50,avg_ttl=1800'

    def test_run_keyspace_stats(self):
        from plugins.valkey.checks import keyspace_stats
        report, data = keyspace_stats.run_keyspace_stats(self.connector, self.settings)
        self.assertIsInstance(report, str)
        self.assertIsInstance(data, dict)
        self.assertIn('keyspace_stats', data)
        self.assertEqual(data['keyspace_stats']['status'], 'success')

if __name__ == '__main__':
    unittest.main()