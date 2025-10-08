import unittest
from unittest.mock import MagicMock

class TestPersistenceSettingsCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}
        self.connector.execute_query.return_value = ("Mocked AsciiDoc output", {"mocked": "data"})

    def test_run_persistence_check(self):
        from plugins.valkey.checks.persistence_settings import run_persistence_check
        adoc_output, structured_data = run_persistence_check(self.connector, self.settings)
        self.assertIsInstance(adoc_output, str)
        self.assertIsInstance(structured_data, dict)
        self.assertIn('persistence_settings', structured_data)
        self.assertIn('recommendations', structured_data)

if __name__ == '__main__':
    unittest.main()