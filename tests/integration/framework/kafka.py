"""Kafka container for integration testing - Docker, Podman, and external server support."""

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


class KafkaContainer(DatabaseContainer):
    """
    Kafka test container - works with Docker, Podman, or external server.
    
    Uses Redpanda for fast startup and lightweight testing (Kafka API compatible).
    
    Configuration via environment variables:
    - KAFKA_TEST_HOST: External server hostname (if set, uses external server)
    - KAFKA_TEST_PORT: External server port (default: 9092)
    - KAFKA_TEST_ADMIN_PORT: Admin API port (default: 9644 for Redpanda, 9092 for Kafka)
    - KAFKA_TEST_USE_REDPANDA: Use Redpanda instead of Kafka (default: true)
    """
    
    def __init__(self, version: str = "latest"):
        """
        Initialize Kafka/Redpanda container or external connection.
        
        Args:
            version: Kafka/Redpanda version (default: latest)
        """
        # Use Redpanda by default for faster startup
        use_redpanda = os.environ.get('KAFKA_TEST_USE_REDPANDA', 'true').lower() == 'true'
        
        if use_redpanda:
            super().__init__('redpanda', version)
            self.engine = 'redpanda'
        else:
            super().__init__('kafka', version)
            self.engine = 'kafka'
        
        # Check if using external server
        self.external_host = os.environ.get('KAFKA_TEST_HOST')
        
        if self.external_host:
            # External server mode
            self.mode = 'external'
            self.host = self.external_host
            self.port = int(os.environ.get('KAFKA_TEST_PORT', 9092))
            self.admin_port = int(os.environ.get('KAFKA_TEST_ADMIN_PORT', 9644))
            
            logger.info(f"Using external Kafka server: {self.host}:{self.port}")
        else:
            # Container mode
            self.runtime = detect_container_runtime()
            if not self.runtime:
                raise RuntimeError(
                    "No container runtime found and no external server configured.\n"
                    "Either:\n"
                    "1. Install Docker or Podman\n"
                    "OR\n"
                    "2. Configure external Kafka server:\n"
                    "   export KAFKA_TEST_HOST=your-kafka-server.com\n"
                    "   export KAFKA_TEST_PORT=9092"
                )
            
            self.mode = 'container'
            self.container_name = f"kafka_healthcheck_test_{self.engine}_{self.runtime}"
            self.container_id = None
            self.host = "localhost"
            self.port = 9092
            self.admin_port = 9644  # Redpanda admin API
        
        self.connector = None
    
    def _run_command(self, args, **kwargs):
        """Run container command with detected runtime."""
        if self.mode != 'container':
            raise RuntimeError("Container commands not available in external server mode")
        cmd = [self.runtime] + args
        return subprocess.run(cmd, **kwargs)
    
    def start(self) -> 'KafkaContainer':
        """Start Kafka/Redpanda container or verify external server connection."""
        if self._started:
            logger.warning("Container/connection already started")
            return self
        
        if self.mode == 'external':
            # External server mode - just verify connection
            logger.info(f"Verifying connection to external Kafka at {self.host}:{self.port}...")
            try:
                self.wait_for_ready(timeout=10)
                self._started = True
                logger.info(f"Successfully connected to external Kafka server")
                return self
            except Exception as e:
                raise RuntimeError(
                    f"Failed to connect to external Kafka server at {self.host}:{self.port}\n"
                    f"Error: {e}"
                )
        
        # Container mode
        if self.engine == 'redpanda':
            return self._start_redpanda()
        else:
            return self._start_kafka()
    
    def _start_redpanda(self) -> 'KafkaContainer':
        """Start Redpanda container (fast Kafka-compatible broker)."""
        logger.info(f"Starting Redpanda container with {self.runtime}...")
        logger.info("Redpanda starts in ~5 seconds (much faster than Kafka)")
        
        try:
            # Remove any existing container
            self._run_command(
                ["rm", "-f", self.container_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            # Build image name
            if self.version == "latest":
                image_name = "docker.io/redpandadata/redpanda:latest"
            else:
                image_name = f"docker.io/redpandadata/redpanda:v{self.version}"


            
            # Start Redpanda container
            cmd = [
                "run", "-d",
                "--name", self.container_name,
                "-p", f"{self.port}:9092",
                "-p", f"{self.admin_port}:9644",
                image_name,
                "redpanda", "start",
                "--overprovisioned",
                "--smp", "1",
                "--memory", "1G",
                "--reserve-memory", "0M",
                "--node-id", "0",
                "--check=false",
                "--kafka-addr", "PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092",
                "--advertise-kafka-addr", f"PLAINTEXT://localhost:29092,OUTSIDE://localhost:{self.port}"
            ]
            
            result = self._run_command(cmd, capture_output=True, text=True, check=True)
            self.container_id = result.stdout.strip()
            
            logger.info(f"Container started: {self.container_id[:12]}")
            
            # Wait for Redpanda to be ready
            self.wait_for_ready(timeout=30)
            
            # Create test topic
            self._create_test_topic()
            
            self._started = True
            logger.info(f"Redpanda container ready (runtime: {self.runtime})")
            return self
        
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start container: {e.stderr}")
            raise RuntimeError(
                f"Container start failed. Check that {self.runtime} is running.\n"
                f"Error: {e.stderr}"
            )
    
    def _start_kafka(self) -> 'KafkaContainer':
        """Start Apache Kafka container (requires ZooKeeper, slower startup)."""
        logger.warning("Starting full Kafka with ZooKeeper - this takes 30-60 seconds")
        logger.warning("Consider using Redpanda instead for faster tests (KAFKA_TEST_USE_REDPANDA=true)")
        
        # For simplicity, we'll use a single-node Kafka image
        # In production tests, you'd want separate ZooKeeper and Kafka containers
        
        try:
            # Remove any existing container
            self._run_command(
                ["rm", "-f", self.container_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            # Using wurstmeister/kafka image which includes ZooKeeper
            image_name = "docker.io/wurstmeister/kafka:latest"
            
            cmd = [
                "run", "-d",
                "--name", self.container_name,
                "-p", f"{self.port}:9092",
                "-e", "KAFKA_ADVERTISED_HOST_NAME=localhost",
                "-e", f"KAFKA_ADVERTISED_PORT={self.port}",
                "-e", "KAFKA_ZOOKEEPER_CONNECT=localhost:2181",
                "-e", "KAFKA_CREATE_TOPICS=test-topic:1:1",
                image_name
            ]
            
            result = self._run_command(cmd, capture_output=True, text=True, check=True)
            self.container_id = result.stdout.strip()
            
            logger.info(f"Container started: {self.container_id[:12]}")
            logger.info("Waiting for Kafka to initialize (30-60 seconds)...")
            
            self.wait_for_ready(timeout=90)
            
            self._started = True
            logger.info(f"Kafka container ready (runtime: {self.runtime})")
            return self
        
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to start container: {e.stderr}")
            raise RuntimeError(f"Container start failed: {e.stderr}")
    
    def stop(self):
        """Stop and remove Kafka container (no-op for external server)."""
        if self.connector:
            try:
                self.connector.close()
            except:
                pass
            self.connector = None
        
        if self.mode == 'external':
            logger.info("Disconnected from external Kafka server")
            self._started = False
            return
        
        # Container mode - stop and remove
        if self.container_id and self._started:
            logger.info(f"Stopping Kafka container ({self.runtime})...")
            try:
                self._run_command(
                    ["stop", self.container_name],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=20
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
        """Get Kafka connector instance."""
        if not self._started:
            raise RuntimeError("Container not started. Call start() first.")
        
        if self.connector is None:
            from plugins.kafka.connector import KafkaConnector
            
            # Build connection settings dict
            settings = {
                'bootstrap_servers': f"{self.host}:{self.port}",
                'client_id': 'pg_healthcheck_test'
            }
            
            self.connector = KafkaConnector(settings)
            self.connector.connect()
        
        return self.connector
    
    def wait_for_ready(self, timeout: int = 30, check_interval: float = 2.0):
        """Wait for Kafka to be ready."""
        start_time = time.time()
        
        logger.info("Waiting for Kafka to complete initialization...")
        
        while time.time() - start_time < timeout:
            try:
                from kafka import KafkaAdminClient
                from kafka.errors import KafkaError
                
                admin_client = KafkaAdminClient(
                    bootstrap_servers=f"{self.host}:{self.port}",
                    client_id='healthcheck_readiness_probe',
                    request_timeout_ms=5000
                )
                
                # Try to list topics
                topics = admin_client.list_topics()
                
                admin_client.close()
                
                elapsed = time.time() - start_time
                logger.info(f"Kafka is ready! (took {elapsed:.1f}s, {len(topics)} topics)")
                
                time.sleep(1)
                return
                
            except Exception as e:
                elapsed = time.time() - start_time
                if int(elapsed) % 10 == 0:
                    logger.info(f"Still waiting... ({int(elapsed)}s elapsed)")
                logger.debug(f"Connection failed: {type(e).__name__}: {str(e)[:100]}")
            
            time.sleep(check_interval)
        
        raise TimeoutError(f"Kafka not ready after {timeout}s")
    
    def _create_test_topic(self):
        """Create test topic in the cluster."""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            
            admin_client = KafkaAdminClient(
                bootstrap_servers=f"{self.host}:{self.port}",
                client_id='healthcheck_setup'
            )
            
            # Create test topic
            topic = NewTopic(
                name='test-topic',
                num_partitions=3,
                replication_factor=1
            )
            
            admin_client.create_topics([topic])
            logger.info("Created test topic: test-topic (3 partitions)")
            
            admin_client.close()
        except Exception as e:
            # Topic might already exist
            logger.debug(f"Could not create test topic: {e}")
    
    def seed_test_data(self, scenario: str = 'default'):
        """
        Seed Kafka with test data for specific scenarios.
        
        Args:
            scenario: Test scenario name
                - 'default': Empty cluster with test topic
                - 'messages': Produce test messages to topics
                - 'consumer_lag': Create consumer group with lag
                - 'under_replicated': Create topics with insufficient replication (requires multi-broker)
        """
        if self.mode == 'external':
            logger.warning(
                f"Seeding test data on external Kafka server {self.host}:{self.port}"
            )
        
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.admin import KafkaAdminClient, NewTopic
        import json
        
        if scenario == 'default':
            # Just ensure test topic exists
            logger.info("Seeded 'default' scenario (test-topic created)")
        
        elif scenario == 'messages':
            # Produce test messages
            producer = KafkaProducer(
                bootstrap_servers=f"{self.host}:{self.port}",
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            # Produce 1000 messages to test-topic
            for i in range(1000):
                message = {
                    'id': i,
                    'message': f'Test message {i}',
                    'timestamp': time.time()
                }
                producer.send('test-topic', value=message)
            
            producer.flush()
            producer.close()
            
            logger.info("Seeded 'messages' scenario (1000 messages in test-topic)")
        
        elif scenario == 'consumer_lag':
            # Create consumer group with lag
            producer = KafkaProducer(
                bootstrap_servers=f"{self.host}:{self.port}",
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            # Produce 5000 messages
            for i in range(5000):
                producer.send('test-topic', value={'id': i})
            producer.flush()
            producer.close()
            
            # Create consumer that only reads 1000 messages
            consumer = KafkaConsumer(
                'test-topic',
                bootstrap_servers=f"{self.host}:{self.port}",
                group_id='test-consumer-group',
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            # Consume only 1000 messages, leaving 4000 lag
            count = 0
            for message in consumer:
                count += 1
                if count >= 1000:
                    break
            
            consumer.close()
            
            logger.info("Seeded 'consumer_lag' scenario (test-consumer-group has ~4000 message lag)")
        
        elif scenario == 'under_replicated':
            # Create topics with insufficient replication
            # Note: This only works with multi-broker setup
            admin_client = KafkaAdminClient(
                bootstrap_servers=f"{self.host}:{self.port}"
            )
            
            # Try to create topic with RF=3 (will fail with single broker)
            topic = NewTopic(
                name='high-replication-topic',
                num_partitions=6,
                replication_factor=3
            )
            
            try:
                admin_client.create_topics([topic])
                logger.info("Created high-replication-topic (will be under-replicated on single broker)")
            except Exception as e:
                logger.warning(f"Cannot create under-replicated scenario with single broker: {e}")
            
            admin_client.close()
        
        else:
            logger.warning(f"Unknown scenario: {scenario}, using default")
