"""MongoDB container for integration testing - Docker, Podman, and external server support."""

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


class MongoDBContainer(DatabaseContainer):
    """
    MongoDB test container - works with Docker, Podman, or external server.
    
    Configuration via environment variables:
    - MONGODB_TEST_HOST: External server hostname (if set, uses external server)
    - MONGODB_TEST_PORT: External server port (default: 27017)
    - MONGODB_TEST_DB: Database name (default: testdb)
    - MONGODB_TEST_USER: Username (optional)
    - MONGODB_TEST_PASSWORD: Password (optional)
    - MONGODB_TEST_AUTH_SOURCE: Auth database (default: admin)
    - MONGODB_TEST_SSL: Enable SSL (default: false)
    """
    
    def __init__(self, version: str = "7.0"):
        """
        Initialize MongoDB container or external connection.
        
        Args:
            version: MongoDB version (only used for containerized testing)
        """
        super().__init__('mongo', version)
        
        # Check if using external server
        self.external_host = os.environ.get('MONGODB_TEST_HOST')
        
        if self.external_host:
            # External server mode
            self.mode = 'external'
            self.host = self.external_host
            self.port = int(os.environ.get('MONGODB_TEST_PORT', 27017))
            self.db_name = os.environ.get('MONGODB_TEST_DB', 'testdb')
            self.user = os.environ.get('MONGODB_TEST_USER')
            self.password = os.environ.get('MONGODB_TEST_PASSWORD')
            self.auth_source = os.environ.get('MONGODB_TEST_AUTH_SOURCE', 'admin')
            self.use_ssl = os.environ.get('MONGODB_TEST_SSL', 'false').lower() == 'true'
            
            logger.info(f"Using external MongoDB server: {self.host}:{self.port}/{self.db_name}")
        else:
            # Container mode
            self.runtime = detect_container_runtime()
            if not self.runtime:
                raise RuntimeError(
                    "No container runtime found and no external server configured.\n"
                    "Either:\n"
                    "1. Install Docker or Podman\n"
                    "OR\n"
                    "2. Configure external MongoDB server:\n"
                    "   export MONGODB_TEST_HOST=your-server.com\n"
                    "   export MONGODB_TEST_DB=testdb"
                )
            
            self.mode = 'container'
            self.container_name = f"mongo_healthcheck_test_{version}_{self.runtime}"
            self.container_id = None
            self.host = "localhost"
            self.port = 27017
            self.db_name = "testdb"
            self.user = None
            self.password = None
            self.auth_source = "admin"
            self.use_ssl = False
        
        self.connector = None
    
    def _run_command(self, args, **kwargs):
        """Run container command with detected runtime."""
        if self.mode != 'container':
            raise RuntimeError("Container commands not available in external server mode")
        cmd = [self.runtime] + args
        return subprocess.run(cmd, **kwargs)
    
    def start(self) -> 'MongoDBContainer':
        """Start MongoDB container or verify external server connection."""
        if self._started:
            logger.warning("Container/connection already started")
            return self
        
        if self.mode == 'external':
            # External server mode - just verify connection
            logger.info(f"Verifying connection to external MongoDB at {self.host}:{self.port}...")
            try:
                self.wait_for_ready(timeout=10)
                self._started = True
                logger.info(f"Successfully connected to external MongoDB server")
                return self
            except Exception as e:
                raise RuntimeError(
                    f"Failed to connect to external MongoDB server at {self.host}:{self.port}\n"
                    f"Error: {e}"
                )
        
        # Container mode
        logger.info(f"Starting MongoDB {self.version} container with {self.runtime}...")
        
        try:
            # Remove any existing container with same name
            self._run_command(
                ["rm", "-f", self.container_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            # Build image name with registry for Podman compatibility
            image_name = f"docker.io/mongo:{self.version}"
            
            # Start new container (no auth for simplicity in tests)
            cmd = [
                "run", "-d",
                "--name", self.container_name,
                "-p", f"{self.port}:27017",
                image_name
            ]
            
            result = self._run_command(cmd, capture_output=True, text=True, check=True)
            self.container_id = result.stdout.strip()
            
            logger.info(f"Container started: {self.container_id[:12]}")
            
            # Wait for MongoDB to be ready
            self.wait_for_ready()
            
            self._started = True
            logger.info(f"MongoDB container ready (runtime: {self.runtime})")
            return self
        
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start container: {e.stderr}")
            raise RuntimeError(
                f"Container start failed. Check that {self.runtime} is running.\n"
                f"Error: {e.stderr}"
            )
    
    def stop(self):
        """Stop and remove MongoDB container (no-op for external server)."""
        if self.connector:
            try:
                self.connector.close()
            except:
                pass
            self.connector = None
        
        if self.mode == 'external':
            logger.info("Disconnected from external MongoDB server")
            self._started = False
            return
        
        # Container mode - stop and remove
        if self.container_id and self._started:
            logger.info(f"Stopping MongoDB container ({self.runtime})...")
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
        Get MongoDB connector instance.
        
        Returns:
            MongoDBConnector instance
        """
        if not self._started:
            raise RuntimeError("Container not started. Call start() first.")
        
        if self.connector is None:
            from plugins.mongodb.connector import MongoDBConnector
            
            # Build connection parameters
            conn_params = {
                'host': self.host,
                'port': self.port,
                'database': self.db_name
            }
            
            # Add auth if configured
            if self.user and self.password:
                conn_params['user'] = self.user
                conn_params['password'] = self.password
                conn_params['auth_source'] = self.auth_source
            
            if self.use_ssl:
                conn_params['use_ssl'] = True
            
            self.connector = MongoDBConnector(conn_params)
            self.connector.connect()
        
        return self.connector
    
    def wait_for_ready(self, timeout: int = 30, check_interval: float = 0.5):
        """
        Wait for MongoDB to be ready.
        
        Args:
            timeout: Maximum time to wait in seconds
            check_interval: Time between readiness checks
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                from plugins.mongodb.connector import MongoDBConnector
                
                test_params = {
                    'host': self.host,
                    'port': self.port,
                    'database': self.db_name
                }
                
                if self.user and self.password:
                    test_params['user'] = self.user
                    test_params['password'] = self.password
                    test_params['auth_source'] = self.auth_source
                
                test_conn = MongoDBConnector(test_params)
                test_conn.connect()
                # Try a simple command
                test_conn.client.admin.command('ping')
                test_conn.close()
                logger.info(f"MongoDB {self.mode} is ready")
                return
            except Exception as e:
                logger.debug(f"Waiting for MongoDB... ({e})")
            
            time.sleep(check_interval)
        
        connection_info = f"{self.host}:{self.port}" if self.mode == 'external' else f"container {self.container_name}"
        raise TimeoutError(
            f"MongoDB did not become ready within {timeout} seconds. "
            f"Connection: {connection_info}"
        )
    
    def seed_test_data(self, scenario: str = 'default'):
        """
        Seed MongoDB with test data for specific scenarios.
        
        Args:
            scenario: Test scenario name
                - 'default': Empty database
                - 'collections': Create test collections with data
        """
        if self.mode == 'external':
            logger.warning(
                f"Seeding test data on external server {self.host}:{self.port}/{self.db_name}"
            )
        
        connector = self.get_connector()
        db = connector.client[self.db_name]
        
        if scenario == 'default':
            # Just ensure database exists
            db.command('ping')
            logger.info("Seeded 'default' scenario (empty database)")
        
        elif scenario == 'collections':
            # Create test collections
            db.test_collection.drop()
            db.test_collection.insert_many([
                {'name': f'test_{i}', 'value': i, 'active': True}
                for i in range(100)
            ])
            logger.info("Seeded 'collections' scenario")
        
        else:
            logger.warning(f"Unknown scenario: {scenario}, using default")
