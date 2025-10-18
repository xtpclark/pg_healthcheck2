"""Cassandra container for integration testing - Docker, Podman, and external server support."""

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


class CassandraContainer(DatabaseContainer):
    """
    Cassandra test container - works with Docker, Podman, or external server.
    
    Configuration priority (highest to lowest):
    1. Environment variables
    2. settings dict from config.yaml
    3. Container defaults
    
    Environment variables:
    - CASSANDRA_TEST_HOST: External server hostname (if set, uses external server)
    - CASSANDRA_TEST_PORT: External server port (default: 9042)
    - CASSANDRA_TEST_KEYSPACE: Keyspace name (default: test_keyspace)
    - CASSANDRA_TEST_USER: Username (optional)
    - CASSANDRA_TEST_PASSWORD: Password (optional)
    - CASSANDRA_TEST_DATACENTER: Datacenter name (default: datacenter1)
    - CASSANDRA_SSH_HOST: SSH host for nodetool access
    - CASSANDRA_SSH_USER: SSH username
    - CASSANDRA_SSH_KEY: SSH key file path
    - CASSANDRA_SSH_PASSWORD: SSH password (alternative to key)
    - CASSANDRA_SSH_TIMEOUT: SSH timeout in seconds (default: 10)
    """
    
    def __init__(self, version: str = "4.1", settings: dict = None):
        """
        Initialize Cassandra container or external connection.
        
        Args:
            version: Cassandra version (3.11, 4.0, 4.1, 5.0)
            settings: Optional settings dict from config.yaml
        """
        super().__init__('cassandra', version)
        self.settings = settings or {}
        
        # Check if using external server (env vars take precedence over config)
        self.external_host = os.environ.get('CASSANDRA_TEST_HOST')
        
        if not self.external_host:
            # Check config file for external server
            ext_config = self.settings.get('integration_tests', {}).get('external_servers', {}).get('cassandra', {})
            if ext_config and ext_config.get('enabled') and ext_config.get('host'):
                self.external_host = ext_config['host']
                logger.info(f"Using external Cassandra server from config: {self.external_host}")
                
                # Populate environment variables from config if not already set
                # This makes SSH settings available to the connector
                if not os.environ.get('CASSANDRA_TEST_PORT'):
                    os.environ['CASSANDRA_TEST_PORT'] = str(ext_config.get('port', 9042))
                if not os.environ.get('CASSANDRA_TEST_KEYSPACE'):
                    os.environ['CASSANDRA_TEST_KEYSPACE'] = ext_config.get('keyspace', 'test_keyspace')
                if not os.environ.get('CASSANDRA_TEST_DATACENTER'):
                    os.environ['CASSANDRA_TEST_DATACENTER'] = ext_config.get('datacenter', 'datacenter1')
                
                # Read SSH config from file if not in env vars
                ssh_config = ext_config.get('ssh', {})
                if ssh_config:
                    if ssh_config.get('host') and not os.environ.get('CASSANDRA_SSH_HOST'):
                        os.environ['CASSANDRA_SSH_HOST'] = ssh_config['host']
                    if ssh_config.get('user') and not os.environ.get('CASSANDRA_SSH_USER'):
                        os.environ['CASSANDRA_SSH_USER'] = ssh_config['user']
                    if ssh_config.get('key_file') and not os.environ.get('CASSANDRA_SSH_KEY'):
                        os.environ['CASSANDRA_SSH_KEY'] = ssh_config['key_file']
                    if ssh_config.get('password') and not os.environ.get('CASSANDRA_SSH_PASSWORD'):
                        os.environ['CASSANDRA_SSH_PASSWORD'] = ssh_config['password']
                    if ssh_config.get('timeout') and not os.environ.get('CASSANDRA_SSH_TIMEOUT'):
                        os.environ['CASSANDRA_SSH_TIMEOUT'] = str(ssh_config['timeout'])
        
        if self.external_host:
            # External server mode
            self.mode = 'external'
            self.host = self.external_host
            self.port = int(os.environ.get('CASSANDRA_TEST_PORT', 9042))
            self.keyspace = os.environ.get('CASSANDRA_TEST_KEYSPACE', 'test_keyspace')
            self.user = os.environ.get('CASSANDRA_TEST_USER')
            self.password = os.environ.get('CASSANDRA_TEST_PASSWORD')
            self.datacenter = os.environ.get('CASSANDRA_TEST_DATACENTER', 'datacenter1')
            
            # Store SSH config for connector
            ssh_host = os.environ.get('CASSANDRA_SSH_HOST')
            if ssh_host:
                self.ssh_config = {
                    'host': ssh_host,
                    'user': os.environ.get('CASSANDRA_SSH_USER'),
                    'key_file': os.environ.get('CASSANDRA_SSH_KEY'),
                    'password': os.environ.get('CASSANDRA_SSH_PASSWORD'),
                    'timeout': int(os.environ.get('CASSANDRA_SSH_TIMEOUT', 10))
                }
                logger.info(f"SSH configured: {self.ssh_config['user']}@{self.ssh_config['host']}")
            else:
                self.ssh_config = None
                logger.warning("No SSH configuration found - nodetool checks will not work")
            
            logger.info(f"Using external Cassandra server: {self.host}:{self.port}/{self.keyspace}")
        else:
            # Container mode
            self.runtime = detect_container_runtime()
            if not self.runtime:
                raise RuntimeError(
                    "No container runtime found and no external server configured.\n"
                    "Either:\n"
                    "1. Install Docker or Podman\n"
                    "OR\n"
                    "2. Configure external Cassandra server:\n"
                    "   export CASSANDRA_TEST_HOST=your-server.com\n"
                    "   export CASSANDRA_TEST_KEYSPACE=test_keyspace"
                )
            
            self.mode = 'container'
            self.container_name = f"cassandra_healthcheck_test_{version.replace('.', '_')}_{self.runtime}"
            self.container_id = None
            self.host = "localhost"
            self.port = 9042
            self.keyspace = "test_keyspace"
            self.user = None
            self.password = None
            self.datacenter = "datacenter1"
            self.ssh_config = None  # No SSH in container mode
        
        self.connector = None
    
    def _run_command(self, args, **kwargs):
        """Run container command with detected runtime."""
        if self.mode != 'container':
            raise RuntimeError("Container commands not available in external server mode")
        cmd = [self.runtime] + args
        return subprocess.run(cmd, **kwargs)
    
    def start(self) -> 'CassandraContainer':
        """Start Cassandra container or verify external server connection."""
        if self._started:
            logger.warning("Container/connection already started")
            return self
        
        if self.mode == 'external':
            # External server mode - just verify connection
            logger.info(f"Verifying connection to external Cassandra at {self.host}:{self.port}...")
            try:
                self.wait_for_ready(timeout=10)
                self._started = True
                logger.info(f"Successfully connected to external Cassandra server")
                return self
            except Exception as e:
                raise RuntimeError(
                    f"Failed to connect to external Cassandra server at {self.host}:{self.port}\n"
                    f"Error: {e}"
                )
        
        # Container mode
        logger.info(f"Starting Cassandra {self.version} container with {self.runtime}...")
        logger.warning("Cassandra takes 30-60 seconds to start - please be patient...")
        
        try:
            # Remove any existing container with same name
            self._run_command(
                ["rm", "-f", self.container_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            # Build image name with registry for Podman compatibility
            image_name = f"docker.io/cassandra:{self.version}"
            
            # Start new container with environment variables
            cmd = [
                "run", "-d",
                "--name", self.container_name,
                "-p", f"{self.port}:9042",
                "-e", "CASSANDRA_CLUSTER_NAME=TestCluster",
                "-e", f"CASSANDRA_DC={self.datacenter}",
                "-e", "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch",
                image_name
            ]
            
            result = self._run_command(cmd, capture_output=True, text=True, check=True)
            self.container_id = result.stdout.strip()
            
            logger.info(f"Container started: {self.container_id[:12]}")
            logger.info("Waiting for Cassandra to initialize (this may take 30-120 seconds)...")
            
            # Wait for Cassandra to be ready
            self.wait_for_ready(timeout=120)
            
            # Create test keyspace
            self._create_test_keyspace()
            
            self._started = True
            logger.info(f"Cassandra container ready (runtime: {self.runtime})")
            return self
        
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start container: {e.stderr}")
            raise RuntimeError(
                f"Container start failed. Check that {self.runtime} is running.\n"
                f"Error: {e.stderr}"
            )
    
    def stop(self):
        """Stop and remove Cassandra container (no-op for external server)."""
        if self.connector:
            try:
                self.connector.close()
            except:
                pass
            self.connector = None
        
        if self.mode == 'external':
            logger.info("Disconnected from external Cassandra server")
            self._started = False
            return
        
        # Container mode - stop and remove
        if self.container_id and self._started:
            logger.info(f"Stopping Cassandra container ({self.runtime})...")
            try:
                self._run_command(
                    ["stop", self.container_name],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=30
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
        """Get Cassandra connector instance with SSH config if available."""
        if not self._started:
            raise RuntimeError("Container not started. Call start() first.")
        
        if self.connector is None:
            from plugins.cassandra.connector import CassandraConnector
            
            # Build connection settings dict
            if self.mode == 'external':
                # External server - use actual host
                settings = {
                    'hosts': [self.host],
                    'port': self.port,
                    'keyspace': self.keyspace,
                    'datacenter': self.datacenter
                }
            else:
                # Container mode - use localhost
                settings = {
                    'hosts': ['127.0.0.1'],
                    'port': self.port,
                    'keyspace': self.keyspace,
                    'datacenter': self.datacenter
                }
            
            # Add auth if configured
            if self.user and self.password:
                settings['user'] = self.user
                settings['password'] = self.password
            
            # Add SSH settings if available (external mode only)
            if self.mode == 'external' and self.ssh_config:
                settings['ssh_host'] = self.ssh_config.get('host')
                settings['ssh_user'] = self.ssh_config.get('user')
                settings['ssh_key_file'] = self.ssh_config.get('key_file')
                settings['ssh_password'] = self.ssh_config.get('password')
                settings['ssh_timeout'] = self.ssh_config.get('timeout', 10)
                logger.info(f"Connector configured with SSH: {settings['ssh_user']}@{settings['ssh_host']}")
            
            self.connector = CassandraConnector(settings)
            self.connector.connect()
        
        return self.connector
    
    def wait_for_ready(self, timeout: int = 120, check_interval: float = 3.0):
        """Wait for Cassandra to be ready."""
        start_time = time.time()
        
        logger.info("Waiting for Cassandra to complete initialization...")
        
        while time.time() - start_time < timeout:
            try:
                from cassandra.cluster import Cluster
                from cassandra.policies import DCAwareRoundRobinPolicy
                from cassandra.query import dict_factory
                
                # Use appropriate host based on mode
                contact_host = self.host if self.mode == 'external' else '127.0.0.1'
                
                cluster = Cluster(
                    contact_points=[contact_host],
                    port=self.port,
                    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=self.datacenter),
                    connect_timeout=30,
                    control_connection_timeout=30,
                    protocol_version=4
                )
                
                session = cluster.connect(wait_for_all_pools=True)
                session.row_factory = dict_factory
                
                # Query version
                result = session.execute("SELECT release_version FROM system.local")
                version = list(result)[0]['release_version']
                
                session.shutdown()
                cluster.shutdown()
                
                elapsed = time.time() - start_time
                logger.info(f"Cassandra is ready! (took {elapsed:.1f}s, version: {version})")
                
                time.sleep(2)
                return
                
            except Exception as e:
                elapsed = time.time() - start_time
                if int(elapsed) % 15 == 0:
                    logger.info(f"Still waiting... ({int(elapsed)}s elapsed)")
                logger.debug(f"Connection failed: {type(e).__name__}: {str(e)[:100]}")
            
            time.sleep(check_interval)
        
        raise TimeoutError(f"Cassandra not ready after {timeout}s")
    
    def _create_test_keyspace(self):
        """Create test keyspace in the container."""
        try:
            from cassandra.cluster import Cluster
            from cassandra.policies import DCAwareRoundRobinPolicy
            
            cluster = Cluster(
                contact_points=['127.0.0.1'],
                port=self.port,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=self.datacenter),
                connect_timeout=30,
                control_connection_timeout=30,
                protocol_version=4
            )
            
            session = cluster.connect()
            
            # Create keyspace if it doesn't exist
            session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            
            logger.info(f"Created test keyspace: {self.keyspace}")
            
            session.shutdown()
            cluster.shutdown()
        except Exception as e:
            logger.warning(f"Could not create test keyspace: {e}")
    
    def seed_test_data(self, scenario: str = 'default'):
        """
        Seed Cassandra with test data for specific scenarios.
        
        Args:
            scenario: Test scenario name
                - 'default': Empty keyspace with test keyspace created
                - 'tables': Create test tables with data
                - 'tombstones': Create tables with many tombstones
        """
        if self.mode == 'external':
            logger.warning(
                f"Seeding test data on external server {self.host}:{self.port}/{self.keyspace}"
            )
        
        connector = self.get_connector()
        session = connector.session
        
        if scenario == 'default':
            # Just ensure keyspace exists
            logger.info("Seeded 'default' scenario (empty keyspace)")
        
        elif scenario == 'tables':
            # Create test tables
            session.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.keyspace}.test_users (
                    user_id UUID PRIMARY KEY,
                    username TEXT,
                    email TEXT,
                    created_at TIMESTAMP
                )
            """)
            
            # Insert test data
            from uuid import uuid4
            from datetime import datetime
            
            for i in range(100):
                session.execute(
                    f"""
                    INSERT INTO {self.keyspace}.test_users (user_id, username, email, created_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (uuid4(), f'user_{i}', f'user{i}@test.com', datetime.now())
                )
            
            logger.info("Seeded 'tables' scenario (test_users table with 100 rows)")
        
        elif scenario == 'tombstones':
            # Create table and insert/delete data to create tombstones
            session.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.keyspace}.tombstone_test (
                    pk INT PRIMARY KEY,
                    data TEXT
                )
            """)
            
            # Insert and delete to create tombstones
            for i in range(1000):
                session.execute(
                    f"INSERT INTO {self.keyspace}.tombstone_test (pk, data) VALUES (%s, %s)",
                    (i, f'data_{i}')
                )
            
            # Delete most records to create tombstones
            for i in range(0, 900):
                session.execute(
                    f"DELETE FROM {self.keyspace}.tombstone_test WHERE pk = %s",
                    (i,)
                )
            
            logger.info("Seeded 'tombstones' scenario (table with 900 tombstones)")
        
        else:
            logger.warning(f"Unknown scenario: {scenario}, using default")
