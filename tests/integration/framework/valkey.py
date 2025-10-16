"""Valkey/Redis container for integration testing - Docker, Podman, and external server support."""

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


class ValkeyContainer(DatabaseContainer):
    """
    Valkey/Redis test container - works with Docker, Podman, or external server.
    
    Configuration via environment variables:
    - VALKEY_TEST_HOST: External server hostname (if set, uses external server)
    - VALKEY_TEST_PORT: External server port (default: 6379)
    - VALKEY_TEST_PASSWORD: Password (optional)
    - VALKEY_TEST_DB: Database number (default: 0)
    """
    
    def __init__(self, version: str = "7.2", use_redis: bool = False):
        """
        Initialize Valkey/Redis container or external connection.
        
        Args:
            version: Valkey/Redis version
            use_redis: If True, use Redis instead of Valkey
        """
        image_name = 'redis' if use_redis else 'valkey'
        super().__init__(image_name, version)
        
        self.use_redis = use_redis
        
        # Check if using external server
        self.external_host = os.environ.get('VALKEY_TEST_HOST') or os.environ.get('REDIS_TEST_HOST')
        
        if self.external_host:
            # External server mode
            self.mode = 'external'
            self.host = self.external_host
            self.port = int(os.environ.get('VALKEY_TEST_PORT') or os.environ.get('REDIS_TEST_PORT', 6379))
            self.password = os.environ.get('VALKEY_TEST_PASSWORD') or os.environ.get('REDIS_TEST_PASSWORD')
            self.db = int(os.environ.get('VALKEY_TEST_DB') or os.environ.get('REDIS_TEST_DB', 0))
            
            logger.info(f"Using external {image_name.title()} server: {self.host}:{self.port}/{self.db}")
        else:
            # Container mode
            self.runtime = detect_container_runtime()
            if not self.runtime:
                raise RuntimeError(
                    "No container runtime found and no external server configured.\n"
                    "Either:\n"
                    "1. Install Docker or Podman\n"
                    "OR\n"
                    "2. Configure external server:\n"
                    "   export VALKEY_TEST_HOST=your-server.com"
                )
            
            self.mode = 'container'
            self.container_name = f"{image_name}_healthcheck_test_{version}_{self.runtime}"
            self.container_id = None
            self.host = "localhost"
            self.port = 6379
            self.password = None  # No password for test container
            self.db = 0
        
        self.connector = None
    
    def _run_command(self, args, **kwargs):
        """Run container command with detected runtime."""
        if self.mode != 'container':
            raise RuntimeError("Container commands not available in external server mode")
        cmd = [self.runtime] + args
        return subprocess.run(cmd, **kwargs)
    
    def start(self) -> 'ValkeyContainer':
        """Start Valkey/Redis container or verify external server connection."""
        if self._started:
            logger.warning("Container/connection already started")
            return self
        
        image_display = "Redis" if self.use_redis else "Valkey"
        
        if self.mode == 'external':
            # External server mode - just verify connection
            logger.info(f"Verifying connection to external {image_display} at {self.host}:{self.port}...")
            try:
                self.wait_for_ready(timeout=10)
                self._started = True
                logger.info(f"Successfully connected to external {image_display} server")
                return self
            except Exception as e:
                raise RuntimeError(
                    f"Failed to connect to external {image_display} server at {self.host}:{self.port}\n"
                    f"Error: {e}"
                )
        
        # Container mode
        logger.info(f"Starting {image_display} {self.version} container with {self.runtime}...")
        
        try:
            # Remove any existing container with same name
            self._run_command(
                ["rm", "-f", self.container_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            # Build image name with registry for Podman compatibility
            # Valkey is on docker.io, Redis is also on docker.io
            image_name = f"docker.io/{self.image}:{self.version}" if not self.use_redis else f"docker.io/redis:{self.version}"
            
            # Start new container (no password for test)
            cmd = [
                "run", "-d",
                "--name", self.container_name,
                "-p", f"{self.port}:6379",
                image_name
            ]
            
            result = self._run_command(cmd, capture_output=True, text=True, check=True)
            self.container_id = result.stdout.strip()
            
            logger.info(f"Container started: {self.container_id[:12]}")
            
            # Wait for server to be ready
            self.wait_for_ready()
            
            self._started = True
            logger.info(f"{image_display} container ready (runtime: {self.runtime})")
            return self
        
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start container: {e.stderr}")
            raise RuntimeError(
                f"Container start failed. Check that {self.runtime} is running.\n"
                f"Error: {e.stderr}"
            )
    
    def stop(self):
        """Stop and remove Valkey/Redis container (no-op for external server)."""
        if self.connector:
            try:
                self.connector.close()
            except:
                pass
            self.connector = None
        
        image_display = "Redis" if self.use_redis else "Valkey"
        
        if self.mode == 'external':
            logger.info(f"Disconnected from external {image_display} server")
            self._started = False
            return
        
        # Container mode - stop and remove
        if self.container_id and self._started:
            logger.info(f"Stopping {image_display} container ({self.runtime})...")
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
        Get Valkey/Redis connector instance.
        
        Returns:
            ValkeyConnector or RedisConnector instance
        """
        if not self._started:
            raise RuntimeError("Container not started. Call start() first.")
        
        if self.connector is None:
            # Try to import Valkey connector first, fall back to Redis
            try:
                from plugins.valkey.connector import ValkeyConnector
                connector_class = ValkeyConnector
            except ImportError:
                try:
                    from plugins.redis.connector import RedisConnector
                    connector_class = RedisConnector
                except ImportError:
                    raise RuntimeError(
                        "Neither ValkeyConnector nor RedisConnector found. "
                        "Ensure you have a valkey or redis plugin."
                    )
            
            # Build connection parameters
            conn_params = {
                'host': self.host,
                'port': self.port,
                'db': self.db
            }
            
            if self.password:
                conn_params['password'] = self.password
            
            self.connector = connector_class(conn_params)
            self.connector.connect()
        
        return self.connector
    
    def wait_for_ready(self, timeout: int = 30, check_interval: float = 0.5):
        """
        Wait for Valkey/Redis to be ready.
        
        Args:
            timeout: Maximum time to wait in seconds
            check_interval: Time between readiness checks
        """
        start_time = time.time()
        image_display = "Redis" if self.use_redis else "Valkey"
        
        while time.time() - start_time < timeout:
            try:
                import redis
                
                test_conn = redis.Redis(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db,
                    socket_timeout=1,
                    decode_responses=True
                )
                
                # Try PING command
                result = test_conn.ping()
                if result:
                    test_conn.close()
                    logger.info(f"{image_display} {self.mode} is ready")
                    return
            except Exception as e:
                logger.debug(f"Waiting for {image_display}... ({e})")
            
            time.sleep(check_interval)
        
        connection_info = f"{self.host}:{self.port}" if self.mode == 'external' else f"container {self.container_name}"
        raise TimeoutError(
            f"{image_display} did not become ready within {timeout} seconds. "
            f"Connection: {connection_info}"
        )
    
    def seed_test_data(self, scenario: str = 'default'):
        """
        Seed Valkey/Redis with test data for specific scenarios.
        
        Args:
            scenario: Test scenario name
                - 'default': Empty database
                - 'keys': Create test keys with various types
        """
        if self.mode == 'external':
            logger.warning(
                f"Seeding test data on external server {self.host}:{self.port}/{self.db}"
            )
        
        connector = self.get_connector()
        
        if scenario == 'default':
            # Flush database
            connector.client.flushdb()
            logger.info("Seeded 'default' scenario (empty database)")
        
        elif scenario == 'keys':
            # Create various key types
            connector.client.flushdb()
            
            # Strings
            for i in range(100):
                connector.client.set(f'string_key_{i}', f'value_{i}')
            
            # Lists
            for i in range(10):
                connector.client.lpush(f'list_key_{i}', *[f'item_{j}' for j in range(10)])
            
            # Hashes
            for i in range(10):
                connector.client.hset(f'hash_key_{i}', mapping={
                    f'field_{j}': f'value_{j}' for j in range(5)
                })
            
            # Sets
            for i in range(10):
                connector.client.sadd(f'set_key_{i}', *[f'member_{j}' for j in range(5)])
            
            logger.info("Seeded 'keys' scenario (test keys created)")
        
        else:
            logger.warning(f"Unknown scenario: {scenario}, using default")


# Alias for Redis-specific usage
class RedisContainer(ValkeyContainer):
    """Redis container - alias for ValkeyContainer with use_redis=True."""
    
    def __init__(self, version: str = "7.2"):
        super().__init__(version=version, use_redis=True)
