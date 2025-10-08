import unittest
from unittest.mock import MagicMock

from plugins.valkey.checks import connected_clients

class TestConnectedClientsCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}
        self.connector.execute_command.return_value = 'connected_clients=50\nmaxclients=100'

    def test_run_connected_clients(self):
        result, data = connected_clients.run_connected_clients(self.connector, self.settings)
        self.assertIsInstance(result, str)
        self.assertIsInstance(data, dict)
        self.assertIn('client_connections', data)
        self.assertEqual(data['client_connections']['status'], 'success')
        self.assertEqual(data['client_connections']['data']['connected_clients'], 50)

if __name__ == '__main__':
    unittest.main()