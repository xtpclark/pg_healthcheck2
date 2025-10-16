"""Base classes for database container integration testing."""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, Optional
import logging
import time

logger = logging.getLogger(__name__)


class DatabaseContainer(ABC):
    """
    Abstract base class for database test containers.
    
    Each database plugin should implement this interface for its technology.
    """
    
    def __init__(self, image: str, version: str):
        """
        Initialize container configuration.
        
        Args:
            image: Docker image name (e.g., 'postgres', 'mongo', 'redis')
            version: Version tag (e.g., '16', '7.0', '7.2')
        """
        self.image = image
        self.version = version
        self.container = None
        self.connector = None
        self._started = False
    
    @abstractmethod
    def start(self) -> 'DatabaseContainer':
        """
        Start the container and wait for it to be ready.
        
        Returns:
            self for method chaining
        """
        pass
    
    @abstractmethod
    def stop(self):
        """Stop and remove the container."""
        pass
    
    @abstractmethod
    def get_connector(self) -> Any:
        """
        Get a connector instance for this database.
        
        Returns:
            Database-specific connector instance
        """
        pass
    
    @abstractmethod
    def seed_test_data(self, scenario: str = 'default'):
        """
        Seed the database with test data for specific scenarios.
        
        Args:
            scenario: Name of test scenario (e.g., 'healthy', 'bloated', 'high_connections')
        """
        pass
    
    @contextmanager
    def managed(self):
        """
        Context manager for automatic container lifecycle management.
        
        Usage:
            with PostgreSQLContainer('16').managed() as container:
                connector = container.get_connector()
                # ... run tests ...
        """
        try:
            self.start()
            yield self
        finally:
            self.stop()
    
    def wait_for_ready(self, timeout: int = 30, check_interval: float = 0.5):
        """
        Wait for database to be ready to accept connections.
        
        Args:
            timeout: Maximum time to wait in seconds
            check_interval: Time between readiness checks
            
        Raises:
            TimeoutError: If database doesn't become ready in time
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                connector = self.get_connector()
                connector.connect()
                
                # Try a simple query
                connector.execute_query("SELECT 1")
                connector.close()
                
                logger.info(f"{self.__class__.__name__} is ready")
                return
            except Exception as e:
                logger.debug(f"Waiting for database... ({e})")
                time.sleep(check_interval)
        
        raise TimeoutError(f"Database did not become ready within {timeout} seconds")


class HealthcheckValidator:
    """
    Validates health check execution and results.
    
    Provides assertions and validation methods for integration tests.
    """
    
    @staticmethod
    def validate_check_execution(check_function, connector, settings: Dict = None):
        """
        Validates that a check function executes without errors.
        
        Args:
            check_function: The check function to test (e.g., run_vacuum_status)
            connector: Database connector instance
            settings: Optional settings dict
            
        Returns:
            tuple: (adoc_output, structured_data)
            
        Raises:
            AssertionError: If check execution fails validation
        """
        settings = settings or {}
        
        # Execute check
        result = check_function(connector, settings)
        
        # Validate return type
        assert isinstance(result, tuple), \
            f"Check must return tuple, got {type(result)}"
        assert len(result) == 2, \
            f"Check must return 2-tuple, got {len(result)} elements"
        
        adoc_output, structured_data = result
        
        # Validate output types
        assert isinstance(adoc_output, str), \
            f"AsciiDoc output must be string, got {type(adoc_output)}"
        assert isinstance(structured_data, dict), \
            f"Structured data must be dict, got {type(structured_data)}"
        
        # Validate structured data has status
        for key, value in structured_data.items():
            if isinstance(value, dict):
                assert 'status' in value, \
                    f"Section '{key}' missing 'status' field"
                assert value['status'] in ['success', 'error'], \
                    f"Section '{key}' has invalid status: {value['status']}"
        
        return adoc_output, structured_data
    
    @staticmethod
    def validate_query_execution(query_function, connector):
        """
        Validates that a query function executes and returns expected structure.
        
        Args:
            query_function: Query function from qrylib (e.g., get_vacuum_status_query)
            connector: Database connector instance
            
        Returns:
            tuple: (formatted_output, raw_data)
            
        Raises:
            AssertionError: If query execution fails
        """
        # Get query string
        query = query_function(connector)
        assert isinstance(query, str), \
            f"Query function must return string, got {type(query)}"
        assert len(query.strip()) > 0, \
            "Query function returned empty string"
        
        # Execute query
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        # Validate results
        assert formatted is not None, "Formatted output is None"
        assert raw is not None, "Raw data is None"
        
        # Check for errors in formatted output
        assert "[ERROR]" not in formatted, \
            f"Query execution failed:\n{formatted}"
        
        return formatted, raw
    
    @staticmethod
    def validate_expected_columns(raw_data, expected_columns: list):
        """
        Validates that query results contain expected columns.
        
        Args:
            raw_data: Raw query results (list of dicts)
            expected_columns: List of column names that should be present
            
        Raises:
            AssertionError: If expected columns are missing
        """
        if not raw_data:
            logger.warning("No data returned, skipping column validation")
            return
        
        first_row = raw_data[0] if isinstance(raw_data, list) else raw_data
        
        if not isinstance(first_row, dict):
            logger.warning(f"Cannot validate columns for non-dict data: {type(first_row)}")
            return
        
        for column in expected_columns:
            assert column in first_row, \
                f"Expected column '{column}' not found. Available: {list(first_row.keys())}"
