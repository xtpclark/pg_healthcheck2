import psycopg2
import subprocess
import logging
import re
from plugins.base import BasePlugin
from plugins.common.ssh_mixin import SSHSupportMixin
from plugins.common.output_formatters import AsciiDocFormatter

logger = logging.getLogger(__name__)


class PostgresConnector(SSHSupportMixin):
    """
    Enhanced PostgreSQL connector with cross-node and multi-environment support.

    Capabilities:
    - Auto-detects environment (Aurora, RDS, EC2, bare metal)
    - Discovers cluster topology (primary + replicas)
    - Supports cross-node queries
    - CloudWatch metrics for Aurora/RDS
    - SSH support for bare metal/EC2
    """

    def __init__(self, settings):
        self.settings = settings
        self.conn = None  # Primary connection
        self.cursor = None
        self.version_info = {}
        self.has_pgstat = False
        self.has_pgstat_legacy_io_time = False
        self.has_pgstat_new_io_time = False

        # Multi-environment support
        self.environment = None  # 'aurora', 'rds', 'ec2', 'bare_metal'
        self.environment_details = {}

        # Cross-node support
        self.cluster_topology = []
        self.replica_conns = {}  # {node_host: connection}

        # AWS support
        self._rds_client = None
        self._cloudwatch_client = None
        self._aws_region = None

        # Formatters
        self.formatter = AsciiDocFormatter()

        # Initialize SSH support (from mixin)
        self.initialize_ssh()

    def connect(self):
        """
        Enhanced connection with environment detection and topology discovery.
        """
        try:
            # 1. Connect to primary database
            timeout = self.settings.get('statement_timeout', 30000)
            self.conn = psycopg2.connect(
                host=self.settings['host'],
                port=self.settings['port'],
                dbname=self.settings['database'],
                user=self.settings['user'],
                password=self.settings['password'],
                options=f"-c statement_timeout={timeout}"
            )
            self.conn.autocommit = self.settings.get('autocommit', True)
            self.cursor = self.conn.cursor()

            # 2. Get version info
            self.version_info = self._get_version_info()

            # 3. Detect environment
            self.environment, self.environment_details = self._detect_environment()

            # 4. Discover cluster topology
            self.cluster_topology = self._discover_cluster_topology()

            # 5. Connect to replicas if configured
            if self.settings.get('connect_to_replicas', False):
                self._connect_all_replicas()

            # 6. Initialize AWS clients if needed
            if self.environment in ['aurora', 'rds']:
                self._initialize_aws_clients()

            # 7. Connect SSH hosts if configured
            if self.has_ssh_support():
                connected_ssh_hosts = self.connect_all_ssh()
                if connected_ssh_hosts:
                    self._map_ssh_hosts_to_nodes()

            # 8. Check pg_stat_statements capabilities
            self._check_pg_stat_capabilities()

            # 9. Display enhanced connection status
            self._display_connection_status()

        except psycopg2.Error as e:
            print(f"❌ Error connecting to PostgreSQL: {e}")
            raise

    def _detect_environment(self):
        """
        Auto-detect PostgreSQL environment.

        Returns:
            tuple: (environment_type, details)
        """
        details = {}

        # Check for explicit override
        if self.settings.get('environment_override'):
            env = self.settings['environment_override']
            logger.info(f"Using explicit environment override: {env}")
            details['detection_method'] = 'explicit_override'
            return env, details

        # Legacy is_aurora flag support
        if self.settings.get('is_aurora'):
            logger.info("Using legacy 'is_aurora' flag")
            details['detection_method'] = 'legacy_is_aurora_flag'
            return 'aurora', details

        # Detect Aurora via database queries
        aurora_detected, aurora_details = self._detect_aurora()
        if aurora_detected:
            details.update(aurora_details)
            details['detection_method'] = 'aurora_system_functions'
            return 'aurora', details

        # Detect RDS
        rds_detected, rds_details = self._detect_rds()
        if rds_detected:
            details.update(rds_details)
            details['detection_method'] = 'rds_indicators'
            return 'rds', details

        # Detect Patroni
        patroni_detected, patroni_details = self._detect_patroni()
        if patroni_detected:
            details.update(patroni_details)
            details['detection_method'] = 'patroni_indicators'
            details['ha_solution'] = 'patroni'
            return 'patroni', details

        # Default to bare_metal
        details['detection_method'] = 'default'
        return 'bare_metal', details

    def _detect_aurora(self):
        """Detect Aurora using database-level signals."""
        details = {}
        confidence_score = 0

        try:
            cursor = self.conn.cursor()

            # Signal 1: Version string contains 'Aurora'
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            if 'Aurora' in version:
                confidence_score += 40
                details['version_string'] = version
                logger.debug("Aurora detected in version string")

            # Signal 2: aurora_version() function exists
            try:
                cursor.execute("SELECT aurora_version();")
                aurora_version = cursor.fetchone()[0]
                confidence_score += 30
                details['aurora_version'] = aurora_version
                logger.debug(f"aurora_version() returned: {aurora_version}")
            except psycopg2.Error:
                pass

            # Signal 3: RDS-specific GUC parameters
            try:
                cursor.execute("""
                    SELECT name, setting
                    FROM pg_settings
                    WHERE name LIKE 'rds.%' OR name LIKE 'apg.%'
                    LIMIT 5;
                """)
                rds_params = cursor.fetchall()
                if rds_params:
                    confidence_score += 20
                    details['rds_parameters_count'] = len(rds_params)
                    logger.debug(f"Found {len(rds_params)} RDS/Aurora parameters")
            except psycopg2.Error:
                pass

            is_aurora = confidence_score >= 40
            details['confidence_score'] = confidence_score

            if is_aurora:
                logger.info(f"✅ Aurora detected (confidence: {confidence_score}%)")

            return is_aurora, details

        except Exception as e:
            logger.error(f"Error during Aurora detection: {e}")
            return False, {'error': str(e)}

    def _detect_rds(self):
        """Detect standard RDS (non-Aurora) PostgreSQL."""
        details = {}
        confidence_score = 0

        try:
            cursor = self.conn.cursor()

            # Signal 1: AWS credentials in settings
            if self.settings.get('aws_region') and self.settings.get('db_identifier'):
                confidence_score += 20
                details['aws_region'] = self.settings['aws_region']

            # Signal 2: rds_superuser role exists
            try:
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = 'rds_superuser';")
                if cursor.fetchone():
                    confidence_score += 40
                    details['rds_superuser_exists'] = True
                    logger.debug("rds_superuser role found")
            except psycopg2.Error:
                pass

            is_rds = confidence_score >= 40
            details['confidence_score'] = confidence_score

            if is_rds:
                logger.info(f"✅ RDS detected (confidence: {confidence_score}%)")

            return is_rds, details

        except Exception as e:
            logger.error(f"Error during RDS detection: {e}")
            return False, {'error': str(e)}

    def _detect_patroni(self):
        """
        Detect Patroni HA cluster using multiple detection methods.

        Patroni is a template for PostgreSQL HA using Python and a distributed
        configuration store (etcd, Consul, ZooKeeper, or Kubernetes).

        Returns:
            tuple: (is_patroni: bool, details: dict)
        """
        details = {}
        confidence_score = 0
        detection_methods = []

        try:
            cursor = self.conn.cursor()

            # Signal 1: Check for Patroni-created replication slots
            # Patroni typically creates slots named like 'patroni' or with cluster name
            try:
                cursor.execute("""
                    SELECT slot_name, slot_type, active
                    FROM pg_replication_slots
                    WHERE slot_name LIKE '%patroni%'
                       OR slot_name LIKE '%pgsql%'
                       OR slot_name ~ '^[a-z]+-cluster-[0-9]+'
                    LIMIT 5;
                """)
                patroni_slots = cursor.fetchall()
                if patroni_slots:
                    confidence_score += 30
                    details['patroni_replication_slots'] = [
                        {'name': row[0], 'type': row[1], 'active': row[2]}
                        for row in patroni_slots
                    ]
                    detection_methods.append('replication_slots')
                    logger.debug(f"Found {len(patroni_slots)} Patroni-like replication slots")
            except psycopg2.Error as e:
                logger.debug(f"Could not check replication slots: {e}")

            # Signal 2: Check for Patroni-specific application names in pg_stat_activity
            try:
                cursor.execute("""
                    SELECT DISTINCT application_name
                    FROM pg_stat_activity
                    WHERE application_name ILIKE '%patroni%'
                       OR application_name ILIKE '%pgsql%'
                    LIMIT 5;
                """)
                patroni_apps = cursor.fetchall()
                if patroni_apps:
                    confidence_score += 25
                    details['patroni_applications'] = [row[0] for row in patroni_apps]
                    detection_methods.append('application_names')
                    logger.debug(f"Found Patroni application names: {[row[0] for row in patroni_apps]}")
            except psycopg2.Error as e:
                logger.debug(f"Could not check pg_stat_activity: {e}")

            # Signal 3: Check for Patroni REST API (default port 8008)
            # Try to detect if Patroni REST API is accessible
            try:
                import requests
                from requests.exceptions import RequestException

                patroni_host = self.settings.get('host')
                patroni_port = self.settings.get('patroni_port', 8008)

                # Try multiple endpoints
                endpoints_to_check = [
                    f"http://{patroni_host}:{patroni_port}/patroni",
                    f"http://{patroni_host}:{patroni_port}/health",
                    f"http://{patroni_host}:{patroni_port}/leader"
                ]

                for endpoint in endpoints_to_check:
                    try:
                        response = requests.get(endpoint, timeout=2)
                        if response.status_code in [200, 503]:  # 503 = replica in Patroni
                            confidence_score += 35
                            details['patroni_api_endpoint'] = endpoint
                            details['patroni_api_accessible'] = True
                            detection_methods.append('rest_api')

                            # Try to parse response for cluster info
                            try:
                                api_data = response.json()
                                if 'role' in api_data or 'state' in api_data or 'cluster' in api_data:
                                    details['patroni_node_role'] = api_data.get('role', 'unknown')
                                    details['patroni_state'] = api_data.get('state', 'unknown')
                                    details['patroni_cluster'] = api_data.get('cluster', 'unknown')
                                    confidence_score += 10
                                    logger.debug(f"Patroni API returned: role={api_data.get('role')}, state={api_data.get('state')}")
                            except Exception:
                                pass
                            break
                    except RequestException:
                        continue
            except ImportError:
                logger.debug("requests library not available for Patroni API detection")
            except Exception as e:
                logger.debug(f"Could not check Patroni REST API: {e}")

            # Signal 4: Check for Patroni via SSH (if SSH is configured)
            if self.has_ssh_support():
                try:
                    # Check for Patroni process
                    result = self.execute_ssh_command("ps aux | grep -i '[p]atroni' | head -1")
                    if result and result['success'] and result['stdout']:
                        confidence_score += 20
                        details['patroni_process_detected'] = True
                        details['patroni_process_info'] = result['stdout'].strip()[:200]
                        detection_methods.append('process_check')
                        logger.debug("Patroni process detected via SSH")

                    # Check for Patroni config file
                    config_paths = [
                        '/etc/patroni/patroni.yml',
                        '/etc/patroni.yml',
                        '/var/lib/postgresql/patroni.yml'
                    ]
                    for config_path in config_paths:
                        result = self.execute_ssh_command(f"test -f {config_path} && echo 'exists' || echo 'not_found'")
                        if result and result['success'] and 'exists' in result['stdout']:
                            confidence_score += 15
                            details['patroni_config_file'] = config_path
                            detection_methods.append('config_file')
                            logger.debug(f"Patroni config found at {config_path}")
                            break
                except Exception as e:
                    logger.debug(f"Could not check Patroni via SSH: {e}")

            # Signal 5: Check PostgreSQL configuration for Patroni indicators
            try:
                cursor.execute("""
                    SELECT name, setting
                    FROM pg_settings
                    WHERE name IN ('archive_command', 'restore_command', 'primary_conninfo')
                       AND setting ILIKE '%patroni%'
                    LIMIT 5;
                """)
                patroni_settings = cursor.fetchall()
                if patroni_settings:
                    confidence_score += 15
                    details['patroni_config_params'] = {row[0]: row[1] for row in patroni_settings}
                    detection_methods.append('config_params')
                    logger.debug(f"Found Patroni references in PostgreSQL config")
            except psycopg2.Error as e:
                logger.debug(f"Could not check PostgreSQL settings: {e}")

            # Calculate final result
            is_patroni = confidence_score >= 30  # Threshold for Patroni detection
            details['confidence_score'] = confidence_score
            details['detection_methods'] = detection_methods

            if is_patroni:
                logger.info(f"✅ Patroni cluster detected (confidence: {confidence_score}%, methods: {', '.join(detection_methods)})")
            else:
                logger.debug(f"Patroni not detected (confidence: {confidence_score}%)")

            return is_patroni, details

        except Exception as e:
            logger.error(f"Error during Patroni detection: {e}")
            return False, {'error': str(e)}

    def _discover_cluster_topology(self):
        """
        Discover cluster topology (primary + replicas).

        Returns:
            list: Node information dictionaries
        """
        if self.environment == 'aurora':
            return self._discover_aurora_topology()
        else:
            return self._discover_standard_topology()

    def _discover_aurora_topology(self):
        """Discover Aurora cluster topology via RDS API."""
        topology = []

        try:
            # Extract cluster ID from endpoint
            cluster_id = self._extract_cluster_id_from_endpoint()
            if not cluster_id and self.settings.get('db_cluster_id'):
                cluster_id = self.settings['db_cluster_id']

            if not cluster_id:
                logger.warning("Could not determine Aurora cluster ID")
                return topology

            # Initialize boto3 client
            import boto3
            region = self._extract_region_from_endpoint()
            if not region and self.settings.get('aws_region'):
                region = self.settings['aws_region']

            if not region:
                logger.warning("Could not determine AWS region")
                return topology

            rds_client = boto3.client('rds', region_name=region)

            # Get cluster info
            response = rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)
            cluster_info = response['DBClusters'][0]

            # Add cluster-level endpoints
            if cluster_info.get('Endpoint'):
                topology.append({
                    'host': cluster_info['Endpoint'],
                    'port': cluster_info.get('Port', 5432),
                    'role': 'writer',
                    'endpoint_type': 'cluster',
                    'cluster_id': cluster_id
                })

            if cluster_info.get('ReaderEndpoint'):
                topology.append({
                    'host': cluster_info['ReaderEndpoint'],
                    'port': cluster_info.get('Port', 5432),
                    'role': 'reader',
                    'endpoint_type': 'reader_lb',
                    'cluster_id': cluster_id
                })

            # Get instance-specific endpoints
            for member in cluster_info['DBClusterMembers']:
                instance_id = member['DBInstanceIdentifier']
                is_writer = member['IsClusterWriter']

                instance_response = rds_client.describe_db_instances(
                    DBInstanceIdentifier=instance_id
                )
                instance_info = instance_response['DBInstances'][0]

                topology.append({
                    'host': instance_info['Endpoint']['Address'],
                    'port': instance_info['Endpoint']['Port'],
                    'role': 'writer' if is_writer else 'reader',
                    'endpoint_type': 'instance',
                    'instance_id': instance_id,
                    'instance_class': instance_info['DBInstanceClass'],
                    'availability_zone': instance_info['AvailabilityZone'],
                    'status': instance_info['DBInstanceStatus']
                })

            logger.info(f"Discovered Aurora topology: {len(topology)} endpoints")
            return topology

        except Exception as e:
            logger.error(f"Failed to discover Aurora topology: {e}")
            return []

    def _extract_cluster_id_from_endpoint(self):
        """Extract cluster ID from Aurora endpoint."""
        try:
            host = self.settings['host']
            # Pattern: cluster-name.cluster-xxx.region.rds.amazonaws.com
            if '.cluster-' in host:
                parts = host.split('.cluster-')
                if len(parts) >= 2:
                    cluster_name = parts[0]
                    return cluster_name
        except Exception as e:
            logger.debug(f"Could not extract cluster ID from endpoint: {e}")
        return None

    def _extract_region_from_endpoint(self):
        """Extract AWS region from RDS endpoint."""
        try:
            host = self.settings['host']
            # Pattern: xxx.region.rds.amazonaws.com
            if '.rds.amazonaws.com' in host:
                parts = host.split('.')
                if len(parts) >= 4:
                    # parts[-4] should be the region
                    return parts[-4]
        except Exception as e:
            logger.debug(f"Could not extract region from endpoint: {e}")
        return None

    def _discover_standard_topology(self):
        """Discover topology for standard PostgreSQL."""
        topology = []

        try:
            cursor = self.conn.cursor()

            # Get current host (primary)
            cursor.execute("SELECT inet_server_addr(), inet_server_port();")
            result = cursor.fetchone()
            primary_host = result[0] if result and result[0] else self.settings['host']
            primary_port = result[1] if result and result[1] else self.settings['port']

            topology.append({
                'host': str(primary_host),
                'port': primary_port,
                'role': 'writer',
                'endpoint_type': 'instance',
                'state': 'active'
            })

            # Get replicas from pg_stat_replication
            cursor.execute("""
                SELECT
                    client_addr,
                    client_hostname,
                    state,
                    sync_state,
                    COALESCE(
                        EXTRACT(EPOCH FROM (
                            CASE
                                WHEN replay_lsn IS NOT NULL
                                THEN now() - pg_last_xact_replay_timestamp()
                                ELSE NULL
                            END
                        )),
                        0
                    ) AS replication_lag_seconds
                FROM pg_stat_replication
                WHERE client_addr IS NOT NULL;
            """)

            for row in cursor.fetchall():
                topology.append({
                    'host': str(row[0]),
                    'hostname': row[1],
                    'port': self.settings['port'],
                    'role': 'reader',
                    'endpoint_type': 'instance',
                    'state': row[2],
                    'sync_state': row[3],
                    'replication_lag_seconds': float(row[4]) if row[4] else 0
                })

            logger.info(f"Discovered standard topology: {len(topology)} nodes")
            return topology

        except Exception as e:
            logger.error(f"Failed to discover standard topology: {e}")
            return []

    def _connect_all_replicas(self):
        """Connect to all replica nodes for cross-node checks."""
        for node in self.cluster_topology:
            if node['role'] == 'reader' and node['endpoint_type'] == 'instance':
                try:
                    conn = psycopg2.connect(
                        host=node['host'],
                        port=node['port'],
                        database=self.settings['database'],
                        user=self.settings['user'],
                        password=self.settings['password'],
                        connect_timeout=10
                    )
                    conn.autocommit = True
                    self.replica_conns[node['host']] = conn
                    logger.info(f"Connected to replica: {node['host']}")
                except Exception as e:
                    logger.warning(f"Could not connect to replica {node['host']}: {e}")

    def _initialize_aws_clients(self):
        """Initialize boto3 clients for CloudWatch."""
        try:
            import boto3

            region = self._extract_region_from_endpoint()
            if not region and self.settings.get('aws_region'):
                region = self.settings['aws_region']

            if not region:
                logger.warning("AWS region not configured")
                return

            self._aws_region = region
            self._cloudwatch_client = boto3.client('cloudwatch', region_name=region)
            self._rds_client = boto3.client('rds', region_name=region)
            logger.info(f"Initialized AWS clients for region: {region}")

        except ImportError:
            logger.warning("boto3 not installed - CloudWatch metrics unavailable")
        except Exception as e:
            logger.warning(f"Could not initialize AWS clients: {e}")

    def _map_ssh_hosts_to_nodes(self):
        """PostgreSQL-specific SSH host to node mapping."""
        try:
            # Build host-to-role mapping from topology
            host_node_mapping = {}
            for node in self.cluster_topology:
                if node['endpoint_type'] == 'instance':
                    role_label = node['role']
                    if node.get('instance_id'):
                        role_label = f"{node['role']} ({node['instance_id']})"
                    host_node_mapping[node['host']] = role_label

            # Use mixin's mapping method
            self.map_ssh_hosts_to_nodes(host_node_mapping)

        except Exception as e:
            logger.warning(f"Could not map SSH hosts to PostgreSQL nodes: {e}")

    def _display_connection_status(self):
        """Display enhanced connection status."""
        print("✅ Successfully connected to PostgreSQL")

        # Environment display
        env_display = self.environment.upper()
        if 'confidence_score' in self.environment_details:
            score = self.environment_details['confidence_score']
            env_display += f" (confidence: {score}%)"

        detection_method = self.environment_details.get('detection_method', 'unknown')

        print(f"   - Environment: {env_display}")
        print(f"   - Detection: {detection_method}")
        print(f"   - Version: {self.version_info.get('version_string', 'Unknown')}")

        # Aurora-specific info
        if self.environment == 'aurora' and 'aurora_version' in self.environment_details:
            print(f"   - Aurora Version: {self.environment_details['aurora_version']}")

        # Patroni-specific info
        if self.environment == 'patroni':
            print(f"   - HA Solution: Patroni")
            if 'patroni_node_role' in self.environment_details:
                print(f"   - Node Role: {self.environment_details['patroni_node_role']}")
            if 'patroni_state' in self.environment_details:
                print(f"   - State: {self.environment_details['patroni_state']}")
            if 'patroni_cluster' in self.environment_details:
                print(f"   - Cluster: {self.environment_details['patroni_cluster']}")
            if 'patroni_api_endpoint' in self.environment_details:
                print(f"   - REST API: {self.environment_details['patroni_api_endpoint']}")

            # Show detection methods used
            detection_methods = self.environment_details.get('detection_methods', [])
            if detection_methods:
                print(f"   - Detected via: {', '.join(detection_methods)}")

        # Node topology
        if self.cluster_topology:
            # Count nodes by endpoint type
            instance_nodes = [n for n in self.cluster_topology if n['endpoint_type'] == 'instance']
            print(f"   - Cluster: {len(instance_nodes)} instance(s)")

            for node in instance_nodes:
                role_display = node['role'].title()
                if node.get('instance_id'):
                    role_display += f" ({node['instance_id']})"
                if node.get('sync_state'):
                    role_display += f" - {node['sync_state']}"
                print(f"      • {node['host']} - {role_display}")

        # Cross-node connections
        if self.replica_conns:
            print(f"   - Cross-Node: Connected to {len(self.replica_conns)} replica(s)")

        # Capabilities
        capabilities = []
        if self.has_ssh_support():
            capabilities.append("SSH")
        if self._cloudwatch_client:
            capabilities.append("CloudWatch")
        if capabilities:
            print(f"   - Data Sources: {', '.join(capabilities)}")

        # SSH status
        if self.has_ssh_support():
            connected_ssh_hosts = list(self.get_ssh_hosts())
            if connected_ssh_hosts:
                print(f"   - SSH: Connected to {len(connected_ssh_hosts)} host(s)")
                unmapped_hosts = []
                for ssh_host in connected_ssh_hosts:
                    node_id = self.ssh_host_to_node.get(ssh_host)
                    if node_id:
                        print(f"      • {ssh_host} ({node_id})")
                    else:
                        print(f"      • {ssh_host} (⚠️  Not in replication topology)")
                        unmapped_hosts.append(ssh_host)

                if unmapped_hosts:
                    print(f"   ⚠️  WARNING: {len(unmapped_hosts)} SSH host(s) not in replication topology!")

        # pg_stat_statements info
        print(f"   - pg_stat_statements: {'Enabled' if self.has_pgstat else 'Not Found'}")
        if self.has_pgstat:
            io_status = "Not Available"
            if self.has_pgstat_new_io_time:
                io_status = "Available (PG17+ Style)"
            elif self.has_pgstat_legacy_io_time:
                io_status = "Available (Legacy Style)"
            print(f"   - I/O Timings: {io_status}")

    def execute_on_all_nodes(self, query, include_replicas=True):
        """
        Execute query on all nodes (primary + replicas).

        Args:
            query: SQL query to execute
            include_replicas: Whether to query replicas

        Returns:
            dict: {'primary': [rows...], 'replica_host1': [rows...], ...}
        """
        results = {}

        # Execute on primary
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            results['primary'] = cursor.fetchall()
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to execute on primary: {e}")
            results['primary'] = {'error': str(e)}

        # Execute on replicas
        if include_replicas:
            for host, conn in self.replica_conns.items():
                try:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    results[host] = cursor.fetchall()
                    cursor.close()
                except Exception as e:
                    logger.error(f"Failed to execute on {host}: {e}")
                    results[host] = {'error': str(e)}

        return results

    def supports_cross_node_queries(self):
        """Check if cross-node queries are supported."""
        return len(self.replica_conns) > 0

    def get_cluster_endpoints(self):
        """Get distinct connection endpoints."""
        endpoints = {
            'writer': None,
            'reader': None,
            'instances': {'writer': [], 'readers': []}
        }

        for node in self.cluster_topology:
            if node['endpoint_type'] == 'cluster' and node['role'] == 'writer':
                endpoints['writer'] = node['host']
            elif node['endpoint_type'] == 'reader_lb':
                endpoints['reader'] = node['host']
            elif node['endpoint_type'] == 'instance':
                if node['role'] == 'writer':
                    endpoints['instances']['writer'].append(node['host'])
                else:
                    endpoints['instances']['readers'].append(node['host'])

        return endpoints

    def disconnect(self):
        """Closes all database connections."""
        # Close replica connections
        for host, conn in self.replica_conns.items():
            try:
                conn.close()
                logger.debug(f"Closed connection to replica: {host}")
            except Exception as e:
                logger.warning(f"Error closing replica connection {host}: {e}")

        # Close primary connection
        if self.conn:
            self.conn.close()
            print("🔌 Disconnected from PostgreSQL.")

        # Disconnect SSH
        self.disconnect_all_ssh()

    def _get_version_info(self):
        """Get PostgreSQL version information."""
        try:
            self.cursor.execute("SELECT current_setting('server_version_num');")
            version_num = int(self.cursor.fetchone()[0].strip())

            self.cursor.execute("SELECT current_setting('server_version');")
            version_string = self.cursor.fetchone()[0].strip()

            major_version = version_num // 10000

            return {
                'version_num': version_num,
                'version_string': version_string,
                'major_version': major_version,
                'is_pg10_or_newer': major_version >= 10,
                'is_pg11_or_newer': major_version >= 11,
                'is_pg12_or_newer': major_version >= 12,
                'is_pg13_or_newer': major_version >= 13,
                'is_pg14_or_newer': major_version >= 14,
                'is_pg15_or_newer': major_version >= 15,
                'is_pg16_or_newer': major_version >= 16,
                'is_pg17_or_newer': major_version >= 17,
                'is_pg18_or_newer': major_version >= 18
            }
        except Exception:
            return {
                'version_num': 0, 'version_string': 'unknown', 'major_version': 0,
                'is_pg10_or_newer': False, 'is_pg11_or_newer': False,
                'is_pg12_or_newer': False, 'is_pg13_or_newer': False,
                'is_pg14_or_newer': False, 'is_pg15_or_newer': False,
                'is_pg16_or_newer': False, 'is_pg17_or_newer': False,
                'is_pg18_or_newer': False
            }

    def _check_pg_stat_capabilities(self):
        """Checks for the existence and capabilities of the pg_stat_statements extension."""
        try:
            _, ext_exists = self.execute_query(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements');",
                is_check=True, return_raw=True
            )
            self.has_pgstat = (str(ext_exists).lower() in ['t', 'true'])

            if self.has_pgstat:
                query_new = "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_stat_statements' AND column_name = 'shared_blk_read_time');"
                _, col_exists_new = self.execute_query(query_new, is_check=True, return_raw=True)
                self.has_pgstat_new_io_time = (str(col_exists_new).lower() in ['t', 'true'])

                if not self.has_pgstat_new_io_time:
                    query_legacy = "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'pg_stat_statements' AND column_name = 'blk_read_time');"
                    _, col_exists_legacy = self.execute_query(query_legacy, is_check=True, return_raw=True)
                    self.has_pgstat_legacy_io_time = (str(col_exists_legacy).lower() in ['t', 'true'])

        except Exception as e:
            print(f"Warning: Could not check for pg_stat_statements capabilities: {e}")
            self.has_pgstat = False
            self.has_pgstat_legacy_io_time = False
            self.has_pgstat_new_io_time = False

    def get_db_metadata(self):
        """
        Fetches cluster-level metadata including environment information.

        Returns:
            dict: {'version': str, 'db_name': str, 'environment': str, 'environment_details': dict}
        """
        try:
            dbname_query = "SELECT current_database();"
            self.cursor.execute(dbname_query)
            db_name = self.cursor.fetchone()[0].strip()

            return {
                'version': self.version_info.get('version_string', 'N/A'),
                'db_name': db_name,
                'environment': self.environment or 'unknown',
                'environment_details': self.environment_details or {}
            }
        except Exception as e:
            print(f"Warning: Could not fetch database metadata: {e}")
            return {
                'version': self.version_info.get('version_string', 'N/A'),
                'db_name': 'N/A',
                'environment': self.environment or 'unknown',
                'environment_details': self.environment_details or {}
            }

    def execute_query(self, query, params=None, is_check=False, return_raw=False):
        """Executes a query and returns formatted and raw results."""
        try:
            if not self.cursor or self.cursor.closed:
                self.cursor = self.conn.cursor()

            self.cursor.execute(query, params)

            if is_check:
                result = self.cursor.fetchone()[0] if self.cursor.rowcount > 0 else ""
                return (str(result), result) if return_raw else str(result)

            if self.cursor.description is None:
                return ("", []) if return_raw else ""

            columns = [desc[0] for desc in self.cursor.description]
            results = self.cursor.fetchall()
            raw_results = [dict(zip(columns, row)) for row in results]

            if not results:
                return "[NOTE]\n====\nNo results returned.\n====\n", [] if return_raw else ""

            table = ['|===', '|' + '|'.join(columns)]
            for row in results:
                sanitized_row = [str(v).replace('|', '\\|') if v is not None else '' for v in row]
                table.append('|' + '|'.join(sanitized_row))
            table.append('|===')
            formatted = '\n'.join(table)

            return (formatted, raw_results) if return_raw else formatted
        except psycopg2.Error as e:
            if self.conn:
                self.conn.rollback()
            error_str = f"[ERROR]\n====\nQuery failed: {e}\n====\n"
            return (error_str, {"error": str(e), "query": query}) if return_raw else error_str

    def has_select_privilege(self, view_name):
        """Checks if the current user has SELECT privilege on a given view/table."""
        try:
            query = f"SELECT has_table_privilege(current_user, '{view_name}', 'SELECT');"
            _, has_priv = self.execute_query(query, is_check=True, return_raw=True)
            return (str(has_priv).lower() in ['t', 'true'])
        except Exception as e:
            print(f"Warning: Could not check privilege for {view_name}: {e}")
            return False
