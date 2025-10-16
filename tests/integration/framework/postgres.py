"""PostgreSQL container for integration testing - Docker, Podman, and external server support."""

import subprocess
import time
import logging
import shutil
import os
from .base import DatabaseContainer

logger = logging.getLogger(__name__)


def detect_container_runtime():
    """
    Detect which container runtime is available.
    
    Returns:
        str: 'podman', 'docker', or None
    """
    # Check for Podman first (Fedora default)
    if shutil.which('podman'):
        try:
            result = subprocess.run(
                ['podman', '--version'],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                logger.info(f"Detected Podman: {result.stdout.strip()}")
                return 'podman'
        except:
            pass
    
    # Check for Docker
    if shutil.which('docker'):
        try:
            result = subprocess.run(
                ['docker', '--version'],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                logger.info(f"Detected Docker: {result.stdout.strip()}")
                return 'docker'
        except:
            pass
    
    return None


class PostgreSQLContainer(DatabaseContainer):
    """
    PostgreSQL test container - works with Docker, Podman, or external server.
    
    Configuration via environment variables:
    - POSTGRES_TEST_HOST: External server hostname (if set, uses external server)
    - POSTGRES_TEST_PORT: External server port (default: 5432)
    - POSTGRES_TEST_DB: External server database (default: testdb)
    - POSTGRES_TEST_USER: External server username (default: testuser)
    - POSTGRES_TEST_PASSWORD: External server password (required if using external)
    - POSTGRES_TEST_SSL: Enable SSL for external server (default: false)
    """
    
    def __init__(self, version: str = "16"):
        """
        Initialize PostgreSQL container or external connection.
        
        Args:
            version: PostgreSQL version (only used for containerized testing)
        """
        super().__init__('postgres', version)
        
        # Check if using external server
        self.external_host = os.environ.get('POSTGRES_TEST_HOST')
        
        if self.external_host:
            # External server mode
            self.mode = 'external'
            self.host = self.external_host
            self.port = int(os.environ.get('POSTGRES_TEST_PORT', 5432))
            self.db_name = os.environ.get('POSTGRES_TEST_DB', 'testdb')
            self.user = os.environ.get('POSTGRES_TEST_USER', 'testuser')
            self.password = os.environ.get('POSTGRES_TEST_PASSWORD')
            self.ssl_mode = os.environ.get('POSTGRES_TEST_SSL', 'prefer')
            
            if not self.password:
                raise ValueError(
                    "POSTGRES_TEST_PASSWORD environment variable is required when using external server"
                )
            
            logger.info(f"Using external PostgreSQL server: {self.host}:{self.port}/{self.db_name}")
        else:
            # Container mode
            self.runtime = detect_container_runtime()
            if not self.runtime:
                raise RuntimeError(
                    "No container runtime found and no external server configured.\n"
                    "Either:\n"
                    "1. Install Docker or Podman:\n"
                    "   Fedora: sudo dnf install podman\n"
                    "   Ubuntu/Debian: sudo apt install docker.io\n"
                    "   macOS: brew install docker\n"
                    "OR\n"
                    "2. Configure external PostgreSQL server:\n"
                    "   export POSTGRES_TEST_HOST=your-server.com\n"
                    "   export POSTGRES_TEST_USER=testuser\n"
                    "   export POSTGRES_TEST_PASSWORD=secret\n"
                    "   export POSTGRES_TEST_DB=testdb"
                )
            
            self.mode = 'container'
            self.container_name = f"pg_healthcheck_test_{version}_{self.runtime}"
            self.container_id = None
            self.host = "localhost"
            self.port = 5432
            self.db_name = "testdb"
            self.user = "testuser"
            self.password = "testpass"
            self.ssl_mode = "disable"
        
        self.connector = None
    
    def _run_command(self, args, **kwargs):
        """Run container command with detected runtime."""
        if self.mode != 'container':
            raise RuntimeError("Container commands not available in external server mode")
        cmd = [self.runtime] + args
        return subprocess.run(cmd, **kwargs)
    
    def start(self) -> 'PostgreSQLContainer':
        """Start PostgreSQL container or verify external server connection."""
        if self._started:
            logger.warning("Container/connection already started")
            return self
        
        if self.mode == 'external':
            # External server mode - just verify connection
            logger.info(f"Verifying connection to external PostgreSQL at {self.host}:{self.port}...")
            try:
                self.wait_for_ready(timeout=10)
                self._started = True
                logger.info(f"Successfully connected to external PostgreSQL server")
                return self
            except Exception as e:
                raise RuntimeError(
                    f"Failed to connect to external PostgreSQL server at {self.host}:{self.port}\n"
                    f"Error: {e}\n"
                    f"Check your connection settings and ensure the server is accessible."
                )
        
        # Container mode
        logger.info(f"Starting PostgreSQL {self.version} container with {self.runtime}...")
        
        try:
            # Remove any existing container with same name
            self._run_command(
                ["rm", "-f", self.container_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            # Build image name with registry for Podman compatibility
            image_name = f"docker.io/postgres:{self.version}"
            
            # Start new container
            cmd = [
                "run", "-d",
                "--name", self.container_name,
                "-e", f"POSTGRES_USER={self.user}",
                "-e", f"POSTGRES_PASSWORD={self.password}",
                "-e", f"POSTGRES_DB={self.db_name}",
                "-p", f"{self.port}:5432",
                image_name
            ]
            
            result = self._run_command(cmd, capture_output=True, text=True, check=True)
            self.container_id = result.stdout.strip()
            
            logger.info(f"Container started: {self.container_id[:12]}")
            
            # Wait for PostgreSQL to be ready
            self.wait_for_ready()
            
            self._started = True
            logger.info(f"PostgreSQL container ready (runtime: {self.runtime})")
            return self
        
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start container: {e.stderr}")
            raise RuntimeError(
                f"Container start failed. Check that {self.runtime} is running and you have permissions.\n"
                f"Error: {e.stderr}"
            )
    
    def stop(self):
        """Stop and remove PostgreSQL container (no-op for external server)."""
        if self.connector:
            try:
                self.connector.close()
            except:
                pass
            self.connector = None
        
        if self.mode == 'external':
            # External server - just close connection
            logger.info("Disconnected from external PostgreSQL server")
            self._started = False
            return
        
        # Container mode - stop and remove
        if self.container_id and self._started:
            logger.info(f"Stopping PostgreSQL container ({self.runtime})...")
            try:
                self._run_command(
                    ["stop", self.container_name],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10
                )
                self._run_command(
                    ["rm", self.container_name],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5
                )
            except Exception as e:
                logger.warning(f"Error during cleanup: {e}")
            
            self._started = False
    

    def get_connector(self):
        """
        Get PostgreSQL connector instance.
        
        Returns:
            PostgresConnector instance
        """
        if not self._started:
            raise RuntimeError("Container not started. Call start() first.")
        
        if self.connector is None:
            from plugins.postgres.connector import PostgresConnector
            
            # Build connection settings dict (most connectors expect 'settings' dict)
            settings = {
                'host': self.host,
                'port': self.port,
                'database': self.db_name,
                'user': self.user,
                'password': self.password
            }
            
            # Add SSL parameters for external servers
            if self.mode == 'external' and self.ssl_mode != 'disable':
                settings['sslmode'] = self.ssl_mode
            
            self.connector = PostgresConnector(settings)
            self.connector.connect()
        
        return self.connector

    
    def wait_for_ready(self, timeout: int = 30, check_interval: float = 0.5):
        """
        Wait for PostgreSQL to be ready.
        
        Args:
            timeout: Maximum time to wait in seconds
            check_interval: Time between readiness checks
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                if self.mode == 'external':
                    # External server - try to connect
                    from plugins.postgres.connector import PostgresConnector
                    test_conn = PostgresConnector(
                        host=self.host,
                        port=self.port,
                        database=self.db_name,
                        user=self.user,
                        password=self.password,
                        sslmode=self.ssl_mode if self.ssl_mode != 'disable' else None
                    )
                    test_conn.connect()
                    test_conn.execute_query("SELECT 1")
                    test_conn.close()
                    logger.info("External PostgreSQL server is ready")
                    return
                else:
                    # Container - use pg_isready
                    result = self._run_command(
                        ["exec", self.container_name, 
                         "pg_isready", "-U", self.user],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    
                    if result.returncode == 0:
                        logger.info("PostgreSQL container is ready")
                        # Give it one more second to be fully ready
                        time.sleep(1)
                        return
            except Exception as e:
                logger.debug(f"Waiting for PostgreSQL... ({e})")
            
            time.sleep(check_interval)
        
        connection_info = f"{self.host}:{self.port}" if self.mode == 'external' else f"container {self.container_name}"
        raise TimeoutError(
            f"PostgreSQL did not become ready within {timeout} seconds. "
            f"Connection: {connection_info}"
        )
    
    def seed_test_data(self, scenario: str = 'default'):
        """
        Seed PostgreSQL with test data for specific scenarios.
        
        Note: For external servers, this modifies the database.
        Ensure you're using a dedicated test database!
        
        Args:
            scenario: Test scenario name
                - 'default': Minimal setup, empty database
                - 'bloated': Tables with high dead tuples
                - 'missing_indexes': Tables without proper indexes
        """
        if self.mode == 'external':
            logger.warning(
                f"Seeding test data on external server {self.host}:{self.port}/{self.db_name}. "
                "Ensure this is a dedicated test database!"
            )
        
        connector = self.get_connector()
        
        if scenario == 'default':
            # Minimal setup
            try:
                connector.execute_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
            except:
                pass
        
        elif scenario == 'bloated':
            # Create table with dead tuples
            connector.execute_query("""
                DROP TABLE IF EXISTS bloat_test CASCADE;
                CREATE TABLE bloat_test (
                    id SERIAL PRIMARY KEY,
                    data TEXT
                );
            """)
            
            connector.execute_query("""
                INSERT INTO bloat_test (data)
                SELECT 'test_data_' || generate_series(1, 10000);
            """)
            
            connector.execute_query("""
                DELETE FROM bloat_test WHERE id % 2 = 0;
            """)
            
            connector.execute_query("""
                ALTER TABLE bloat_test SET (autovacuum_enabled = false);
            """)
            
            logger.info("Seeded 'bloated' scenario")
        
        elif scenario == 'missing_indexes':
            # Create tables without indexes on foreign keys
            connector.execute_query("""
                DROP TABLE IF EXISTS child_table CASCADE;
                DROP TABLE IF EXISTS parent_table CASCADE;
                
                CREATE TABLE parent_table (
                    id SERIAL PRIMARY KEY,
                    name TEXT
                );
                
                CREATE TABLE child_table (
                    id SERIAL PRIMARY KEY,
                    parent_id INTEGER REFERENCES parent_table(id),
                    data TEXT
                );
            """)
            
            connector.execute_query("""
                INSERT INTO parent_table (name)
                SELECT 'parent_' || generate_series(1, 100);
                
                INSERT INTO child_table (parent_id, data)
                SELECT 
                    (random() * 99 + 1)::int,
                    'child_data_' || generate_series(1, 1000);
            """)
            
            logger.info("Seeded 'missing_indexes' scenario")
        
        else:
            logger.warning(f"Unknown scenario: {scenario}, using default")
