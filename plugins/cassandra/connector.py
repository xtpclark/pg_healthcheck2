import json
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from typing import Dict, List, Optional

# Import shared utilities
from plugins.common import (
    SSHConnectionManager,
    ShellExecutor,
    AsciiDocFormatter,
    SSHSupportMixin
)
from plugins.common.parsers import NodetoolParser

logger = logging.getLogger(__name__)


class CassandraConnector(SSHSupportMixin):
    """
    Handles all direct communication with Cassandra, including CQL, nodetool, and shell commands.
    
    This connector provides a unified interface for:
    1. CQL queries (standard Cassandra queries)
    2. Nodetool commands (via SSH for operational metrics)
    3. Shell commands (via SSH for system-level metrics)
    
    Query Formats:
        CQL: Standard SQL string
            SELECT * FROM system.local;
        
        Nodetool (single node):
            {"operation": "nodetool", "command": "status"}
        
        Nodetool (cluster-wide):
            {"operation": "nodetool_cluster", "command": "tpstats"}
        
        Shell command:
            {"operation": "shell", "command": "df -h"}
    
    SSH Configuration (required for nodetool and shell):
        - ssh_host: Hostname or IP of Cassandra node
        - ssh_user: SSH username
        - ssh_key_file: Path to private key (or ssh_password)
        - ssh_timeout: Connection timeout in seconds (default: 10)
    
    Example:
        connector = CassandraConnector(settings)
        connector.connect()
        
        # CQL query
        result = connector.execute_query("SELECT * FROM system.local")
        
        # Nodetool command
        result = connector.execute_query('{"operation": "nodetool", "command": "status"}')
        
        # Shell command
        result = connector.execute_query('{"operation": "shell", "command": "df -h"}')
    """
    
    def __init__(self, settings):
        self.settings = settings
        self.cluster = None
        self.session = None
        self.version_info = {}
        
        # Initialize SSH support
        self.ssh_manager = None
        self.shell_executor = None
        self.formatter = AsciiDocFormatter()
        self.parser = NodetoolParser()
        
        # Multi-node support
        self.ssh_managers = {}  # Dict of SSHConnectionManager keyed by node IP
        self.cluster_nodes = []  # List of discovered node addresses
        
        # Initialize SSH if configured
        if settings.get('ssh_host'):
            try:
                self.ssh_manager = SSHConnectionManager(settings)
                self.shell_executor = ShellExecutor(self.ssh_manager, self.formatter)
                logger.info("SSH support enabled for Cassandra connector")
            except Exception as e:
                logger.warning(f"SSH configuration present but invalid: {e}")
                self.ssh_manager = None
                self.shell_executor = None

    def connect(self):
        """Establishes a CQL connection to the cluster."""
        try:
            contact_points = self.settings.get('hosts', ['localhost'])
            port = self.settings.get('port', 9042)
            
            auth_provider = None
            if self.settings.get('user') and self.settings.get('password'):
                auth_provider = PlainTextAuthProvider(
                    username=self.settings.get('user'),
                    password=self.settings.get('password')
                )
            
            self.cluster = Cluster(
                contact_points=contact_points,
                port=port,
                auth_provider=auth_provider
            )
            
            self.session = self.cluster.connect()
            self.session.row_factory = dict_factory
            
            # Set keyspace if specified
            keyspace = self.settings.get('keyspace')
            if keyspace:
                self.session.set_keyspace(keyspace)
            
            self.version_info = self._get_version_info()
            
            # Connect SSH if configured
            if self.ssh_manager:
                try:
                    self.ssh_manager.connect()
                    logger.info("SSH connection established")
                except Exception as e:
                    logger.warning(f"SSH connection failed: {e}")
            
            logger.info("Successfully connected to Cassandra")
            logger.info(f"Version: {self.version_info.get('version_string', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise ConnectionError(f"Could not connect to Cassandra: {e}")

    def disconnect(self):
        """Closes the connection and cleans up resources."""
        if self.cluster:
            try:
                self.cluster.shutdown()
                logger.info("Disconnected from Cassandra")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self.cluster = None
                self.session = None
        
        # Disconnect all multi-node SSH connections
        for node_ip, ssh_mgr in list(self.ssh_managers.items()):
            try:
                ssh_mgr.disconnect()
                logger.info(f"Closed SSH connection to {node_ip}")
            except Exception as e:
                logger.warning(f"Error closing SSH connection to {node_ip}: {e}")
        
        self.ssh_managers = {}
        
        # Disconnect main SSH connection
        if self.ssh_manager:
            self.ssh_manager.disconnect()

    def close(self):
        """Alias for disconnect()."""
        self.disconnect()

    def _get_version_info(self):
        """Fetches Cassandra version via CQL."""
        try:
            rows = self.session.execute("SELECT release_version FROM system.local")
            version_string = list(rows)[0]['release_version'] if rows else 'Unknown'
            parts = version_string.split('.')
            major = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else 0
            
            return {
                'version_string': version_string,
                'major_version': major
            }
        except Exception as e:
            logger.warning(f"Could not fetch version: {e}")
            return {'version_string': 'Unknown', 'major_version': 0}

    def get_db_metadata(self):
        """Fetches basic database metadata."""
        keyspace = self.session.keyspace if self.session else 'system'
        return {
            'version': self.version_info.get('version_string', 'N/A'),
            'db_name': keyspace
        }

    def execute_query(self, query, params=None, return_raw=False):
        """
        Executes a CQL query, nodetool command, or shell command based on query format.
        
        Supports three operation types:
        1. CQL queries (standard SQL strings)
        2. Nodetool commands: {"operation": "nodetool", "command": "status"}
        3. Shell commands: {"operation": "shell", "command": "df -h"}
        
        Args:
            query: CQL query string or JSON command
            params: Optional parameters for CQL query
            return_raw: If True, returns tuple (formatted, raw_data)
        
        Returns:
            str or tuple: Formatted AsciiDoc table, or (formatted, raw) if return_raw=True
        """
        try:
            # Check if the query is a JSON command for nodetool/shell
            if query.strip().startswith('{'):
                query_obj = json.loads(query)
                operation = query_obj.get('operation')
                command = query_obj.get('command')
                
                if not command:
                    raise ValueError("Operation requires a 'command' field")
                
                # Route to appropriate handler
                if operation == 'nodetool_cluster':
                    return self._execute_nodetool_cluster_command(command, return_raw)
                elif operation == 'nodetool':
                    return self._execute_nodetool_command(command, return_raw)
                elif operation == 'shell':
                    return self._execute_shell_command(command, return_raw)
                else:
                    raise ValueError(
                        f"Unknown operation: {operation}. "
                        f"Supported operations: 'nodetool', 'nodetool_cluster', 'shell'"
                    )
    
            # Standard CQL execution
            if params:
                rows = self.session.execute(query, params)
            else:
                rows = self.session.execute(query)
            
            raw_results = list(rows)
            formatted = self.formatter.format_table(raw_results)
            
            return (formatted, raw_results) if return_raw else formatted
            
        except json.JSONDecodeError as e:
            error_msg = self.formatter.format_error(f"Invalid JSON in query: {str(e)}")
            logger.error(error_msg)
            return (error_msg, {'error': str(e)}) if return_raw else error_msg
        except Exception as e:
            logger.error(f"Query execution failed: {e}", exc_info=True)
            error_msg = self.formatter.format_error(f"Query failed: {str(e)}")
            return (error_msg, {'error': str(e)}) if return_raw else error_msg
 
    def _execute_shell_command(self, command, return_raw=False):
        """
        Executes a shell command on a remote node via SSH.
        
        Args:
            command: Shell command to execute (e.g., 'df -h', 'free -m')
            return_raw: If True, returns tuple (formatted, raw_data)
        
        Returns:
            str or tuple: Formatted output or (formatted, raw) if return_raw=True
        """
        if not self.ssh_manager:
            raise ConnectionError(
                "SSH not configured. Required settings: ssh_host, ssh_user, "
                "and ssh_key_file or ssh_password"
            )
        
        try:
            # Ensure connection is active
            self.ssh_manager.ensure_connected()
            
            # Execute shell command
            stdout, stderr, exit_code = self.ssh_manager.execute_command(command)
            
            if exit_code != 0 and not stdout:
                raise RuntimeError(f"Shell command failed (exit code {exit_code}): {stderr}")
            
            if not stdout or not stdout.strip():
                logger.warning(f"Empty output from shell command: {command}")
                note_msg = self.formatter.format_note("No output from shell command.")
                return (note_msg, {}) if return_raw else note_msg
            
            # Format the output
            formatted_output = self.formatter.format_shell_output(command, stdout)
            
            # Return structured data
            raw_data = {
                'command': command,
                'output': stdout,
                'stderr': stderr if stderr else None,
                'exit_code': exit_code
            }
            
            return (formatted_output, raw_data) if return_raw else formatted_output

        except Exception as e:
            error_msg = self.formatter.format_error(f"Shell command failed: {str(e)}")
            logger.error(error_msg, exc_info=True)
            return (error_msg, {'error': str(e)}) if return_raw else error_msg
            
    def _execute_nodetool_command(self, command, return_raw=False):
        """
        Executes a nodetool command on a remote node via SSH.
        
        Args:
            command: Nodetool command to execute (e.g., 'status', 'tpstats')
            return_raw: If True, returns tuple (formatted, parsed_data)
        
        Returns:
            str or tuple: Formatted output or (formatted, parsed) if return_raw=True
        """
        if not self.ssh_manager:
            raise ConnectionError(
                "SSH not configured. Required settings: ssh_host, ssh_user, "
                "and ssh_key_file or ssh_password"
            )
        
        try:
            # Ensure connection is active
            self.ssh_manager.ensure_connected()
            
            # Execute nodetool command
            stdout, stderr, exit_code = self.ssh_manager.execute_command(f"nodetool {command}")
            
            if exit_code != 0:
                raise RuntimeError(f"Nodetool command failed: {stderr}")
            
            if not stdout or not stdout.strip():
                logger.warning(f"Empty output from nodetool {command}")
                note_msg = self.formatter.format_note("No output from nodetool command.")
                return (note_msg, {}) if return_raw else note_msg
            
            # Parse the output
            parsed_data = self.parser.parse(command, stdout)
            
            # Format the structured data
            formatted_output = self._format_nodetool_output(command, parsed_data)
            
            return (formatted_output, parsed_data) if return_raw else formatted_output

        except Exception as e:
            error_msg = self.formatter.format_error(f"Nodetool command failed: {str(e)}")
            logger.error(error_msg, exc_info=True)
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _format_nodetool_output(self, command, parsed_data):
        """
        Formats parsed nodetool data into appropriate AsciiDoc output.
        
        Args:
            command: The nodetool command that was executed
            parsed_data: Structured data from parser
        
        Returns:
            str: Formatted AsciiDoc output
        """
        if command == 'compactionstats':
            # Special handling for compactionstats which returns a dict
            pending = parsed_data.get('pending_tasks', 0)
            active = parsed_data.get('active_compactions', [])
            
            output = [f"Pending Tasks: {pending}\n"]
            if active:
                output.append("Active Compactions:\n")
                output.append(self.formatter.format_table(active))
            else:
                output.append(self.formatter.format_note("No active compactions."))
            
            return '\n'.join(output)
        
        elif command == 'gcstats':
            # Special handling for gcstats which returns a single dict
            if not parsed_data:
                return self.formatter.format_note("No GC statistics available.")
            
            # Helper to format values (handle None for NaN)
            def format_value(value):
                if value is None:
                    return "N/A"
                if isinstance(value, int) and value >= 0:
                    return f"{value:,}"
                if isinstance(value, int) and value < 0:
                    return "N/A"
                return str(value)
            
            # Format as a vertical table
            output = [
                "GC Statistics:\n",
                "|===",
                "|Metric|Value",
                f"|Interval (ms)|{format_value(parsed_data.get('interval_ms'))}",
                f"|Max GC Elapsed (ms)|{format_value(parsed_data.get('max_gc_elapsed_ms'))}",
                f"|Total GC Elapsed (ms)|{format_value(parsed_data.get('total_gc_elapsed_ms'))}",
                f"|Stdev GC Elapsed (ms)|{format_value(parsed_data.get('stdev_gc_elapsed_ms'))}",
                f"|GC Reclaimed (MB)|{format_value(parsed_data.get('gc_reclaimed_mb'))}",
                f"|Collections|{format_value(parsed_data.get('collections'))}",
                f"|Direct Memory Bytes|{format_value(parsed_data.get('direct_memory_bytes'))}",
                "|==="
            ]
            
            return '\n'.join(output)
        
        else:
            # For other commands that return list[dict], use standard table format
            if isinstance(parsed_data, list):
                return self.formatter.format_table(parsed_data)
            else:
                return self.formatter.format_note(str(parsed_data))

    def _discover_nodes(self):
        """
        Discover all nodes in the cluster from system tables.
        
        Returns:
            list[str]: List of node IP addresses in the cluster
        """
        nodes = []
        
        try:
            # Get local node
            local = self.session.execute("SELECT broadcast_address, listen_address FROM system.local")
            local_row = list(local)
            if local_row:
                local_addr = (
                    local_row[0].get('broadcast_address') or 
                    local_row[0].get('listen_address')
                )
                if local_addr:
                    nodes.append(str(local_addr))
            
            # Get peer nodes (use peers_v2 for Cassandra 4.x+)
            if self.version_info.get('major_version', 0) >= 4:
                peers = self.session.execute("SELECT peer FROM system.peers_v2")
            else:
                peers = self.session.execute("SELECT peer FROM system.peers")
            
            for peer in peers:
                peer_addr = peer.get('peer')
                if peer_addr:
                    nodes.append(str(peer_addr))

            # Remove duplicates while preserving order
            nodes = list(dict.fromkeys(nodes))
                    
            logger.info(f"Discovered {len(nodes)} nodes in cluster: {nodes}")
            
        except Exception as e:
            logger.error(f"Failed to discover cluster nodes: {e}")
        
        return nodes

    def _get_ssh_manager_for_node(self, node_ip):
        """
        Get or create SSH manager for a specific node.
        
        Args:
            node_ip: IP address of the node
        
        Returns:
            SSHConnectionManager or None if SSH not configured
        """
        # Check if we already have a manager for this node
        if node_ip in self.ssh_managers:
            mgr = self.ssh_managers[node_ip]
            if mgr.is_connected():
                return mgr
            else:
                # Connection is dead, remove it
                try:
                    mgr.disconnect()
                except:
                    pass
                del self.ssh_managers[node_ip]
        
        # Get SSH credentials
        ssh_user = self.settings.get('ssh_user')
        ssh_key_file = self.settings.get('ssh_key_file')
        ssh_password = self.settings.get('ssh_password')
        ssh_timeout = self.settings.get('ssh_timeout', 10)
        
        if not ssh_user:
            logger.warning(f"No SSH user configured for node {node_ip}")
            return None
        
        if not ssh_key_file and not ssh_password:
            logger.warning(f"No SSH credentials configured for node {node_ip}")
            return None
        
        try:
            # Create settings dict for this specific node
            node_settings = {
                'ssh_host': node_ip,
                'ssh_user': ssh_user,
                'ssh_key_file': ssh_key_file,
                'ssh_password': ssh_password,
                'ssh_timeout': ssh_timeout,
                'ssh_port': self.settings.get('ssh_port', 22)
            }
            
            mgr = SSHConnectionManager(node_settings)
            mgr.connect()
            self.ssh_managers[node_ip] = mgr
            logger.info(f"SSH connection established to {node_ip}")
            return mgr
            
        except Exception as e:
            logger.error(f"Failed to establish SSH connection to {node_ip}: {e}")
            return None

    def _execute_nodetool_cluster_command(self, command, return_raw=False):
        """
        Execute nodetool command on all cluster nodes.
        
        Args:
            command: Nodetool command to execute (e.g., 'status', 'tpstats')
            return_raw: If True, returns tuple (formatted, parsed_data)
        
        Returns:
            str or tuple: Formatted output or (formatted, parsed) if return_raw=True
        """
        # Discover nodes if not already done
        if not self.cluster_nodes:
            self.cluster_nodes = self._discover_nodes()
        
        if not self.cluster_nodes:
            error_msg = self.formatter.format_error("No nodes discovered in cluster")
            return (error_msg, {'error': 'No nodes discovered'}) if return_raw else error_msg
        
        results = {}
        
        for node_ip in self.cluster_nodes:
            try:
                # Get SSH manager for this node
                ssh_mgr = self._get_ssh_manager_for_node(node_ip)
                
                if not ssh_mgr:
                    results[node_ip] = {
                        'success': False,
                        'error': 'SSH connection not available'
                    }
                    continue
                
                # Execute nodetool command
                stdout, stderr, exit_code = ssh_mgr.execute_command(f"nodetool {command}")
                
                if exit_code != 0:
                    results[node_ip] = {
                        'success': False,
                        'error': stderr
                    }
                    continue
                
                if not stdout or not stdout.strip():
                    results[node_ip] = {
                        'success': False,
                        'error': 'Empty output from nodetool command'
                    }
                    continue
                
                # Parse the output
                parsed_data = self.parser.parse(command, stdout)
                
                results[node_ip] = {
                    'success': True,
                    'data': parsed_data
                }
                
            except Exception as e:
                logger.error(f"Failed to execute nodetool on {node_ip}: {e}")
                results[node_ip] = {
                    'success': False,
                    'error': str(e)
                }
        
        # Aggregate results
        all_nodes_data = []
        errors = []
        
        for node_ip, result in results.items():
            if result.get('success'):
                data = result['data']
                # Add node_ip to each row for identification
                if isinstance(data, list):
                    for row in data:
                        if isinstance(row, dict):
                            row['node'] = node_ip
                    all_nodes_data.extend(data)
                elif isinstance(data, dict):
                    # Handle compactionstats format
                    data['node'] = node_ip
                    all_nodes_data.append(data)
            else:
                errors.append(f"Node {node_ip}: {result.get('error', 'Unknown error')}")
        
        # Format output
        if errors:
            error_msg = "[WARNING]\n====\nSome nodes failed:\n" + "\n".join(errors) + "\n====\n\n"
        else:
            error_msg = ""
        
        if all_nodes_data:
            formatted = error_msg + self.formatter.format_table(all_nodes_data)
        else:
            formatted = error_msg + self.formatter.format_note("No data returned from any node.")
        
        return (formatted, all_nodes_data) if return_raw else formatted
