import unittest
from unittest.mock import MagicMock
from plugins.clickhouse.checks import active_queries

class TestActiveQueriesCheck(unittest.TestCase):
    def setUp(self):
        self.connector = MagicMock()
        self.settings = {}
        self.connector.execute_query.return_value = ("Formatted Table", [{"query_id": "1", "elapsed": 5.0}])

    def test_run_active_queries_check(self):
        result_adoc, result_data = active_queries.run_active_queries_check(self.connector, self.settings)
        self.assertIsInstance(result_adoc, str)
        self.assertIsInstance(result_data, dict)
        self.assertIn("active_queries", result_data)
        self.assertEqual(result_data["active_queries"]["status"], "success")

    def test_get_weight(self):
        weight = active_queries.get_weight()
        self.assertIsInstance(weight, int)
        self.assertTrue(1 <= weight <= 10)

if __name__ == '__main__':
    unittest.main()
