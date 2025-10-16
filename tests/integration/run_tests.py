#!/usr/bin/env python3
"""
Simple integration test runner.
"""

import sys
import unittest
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def run_postgres_tests():
    """Run PostgreSQL integration tests."""
    loader = unittest.TestLoader()
    suite = loader.discover(
        str(project_root / 'plugins' / 'postgres' / 'integration_tests'),
        pattern='test_*.py'
    )
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

if __name__ == '__main__':
    success = run_postgres_tests()
    sys.exit(0 if success else 1)
