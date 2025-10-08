import unittest
from unittest.mock import MagicMock
from plugins.valkey.checks import memory_pressure

class TestMemoryPressureCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}
        self.connector.execute_command.return_value = {
            'used_memory': '1000000',
            'maxmemory': '2000000',
            'evicted_keys': '500'
        }

    def test_run_memory_pressure_check(self):
        result, structured_data = memory_pressure.run_memory_pressure_check(self.connector, self.settings)
        self.assertIsInstance(result, str)
        self.assertIsInstance(structured_data, dict)
        self.assertIn('memory_pressure', structured_data)
        self.assertIn('status', structured_data['memory_pressure'])
        self.assertIn('data', structured_data['memory_pressure'])

    def test_get_weight(self):
        weight = memory_pressure.get_weight()
        self.assertIsInstance(weight, int)
        self.assertTrue(1 <= weight <= 10)

if __name__ == '__main__':
    unittest.main()