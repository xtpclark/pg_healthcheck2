import unittest
from unittest.mock import MagicMock

import sys
sys.path.append('plugins/valkey')
from checks.replication_status import run_replication_status, get_weight

class TestReplicationStatusCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}
        self.connector.execute_command.return_value = {
            'role': 'master',
            'connected_slaves': '1',
            'slave0': 'ip=127.0.0.1,port=6380,state=online',
            'master_repl_offset': '1000',
            'slave0_repl_offset': '900'
        }

    def test_get_weight(self):
        weight = get_weight()
        self.assertIsInstance(weight, int)
        self.assertTrue(1 <= weight <= 10)

    def test_run_replication_status(self):
        report, data = run_replication_status(self.connector, self.settings)
        self.assertIsInstance(report, str)
        self.assertIsInstance(data, dict)
        self.assertIn('replication_status', data)
        self.assertEqual(data['replication_status']['status'], 'success')
        self.assertIn('Replication Status and Lag Check', report)

if __name__ == '__main__':
    unittest.main()