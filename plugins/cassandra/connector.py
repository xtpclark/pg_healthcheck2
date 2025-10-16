import json
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory

# Paramiko is used for SSH connections to run nodetool
try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

logger = logging.getLogger(__name__)

class CassandraConnector:
    """Handles all direct communication with Cassandra, including CQL and nodetool."""

    def __init__(self, settings):
        self.settings = settings
        self.cluster = None
        self.session = None
        self.version_info = {}

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
            self.session.row_factory = dict_factory  # Return dicts
            
            # Set keyspace if specified
            keyspace = self.settings.get('keyspace')
            if keyspace:
                self.session.set_keyspace(keyspace)
            
            self.version_info = self._get_version_info()
            
            print("✅ Successfully connected to Cassandra.")
            print(f"   - Version: {self.version_info.get('version_string', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise ConnectionError(f"Could not connect to Cassandra: {e}")

    def disconnect(self):
        """Closes the connection."""
        if self.cluster:
            try:
                self.cluster.shutdown()
                print("🔌 Disconnected from Cassandra.")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self.cluster = None
                self.session = None

    def close(self):
        """Alias for disconnect()."""
        self.disconnect()

    def _get_version_info(self):
        """Fetches Cassandra version via CQL."""
        try:
            rows = self.session.execute("SELECT release_version FROM system.local")
            version_string = list(rows)[0]['release_version'] if rows else 'Unknown'
            parts = version_string.split('.')
            major = int(parts[0]) if len(parts) > 0 else 0
            
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
        Executes a CQL query or a nodetool command based on query format.
        """
        try:
            # Check if the query is a JSON command for nodetool
            if query.strip().startswith('{'):
                query_obj = json.loads(query)
                if query_obj.get('operation') == 'nodetool':
                    command = query_obj.get('command')
                    if not command:
                        raise ValueError("Nodetool operation requires a 'command'")
                    return self._execute_nodetool_command(command, return_raw)

            # --- If not nodetool, proceed with standard CQL execution ---
            if params:
                rows = self.session.execute(query, params)
            else:
                rows = self.session.execute(query)
            
            raw_results = list(rows)
            formatted = self._format_results_as_table(raw_results)
            
            return (formatted, raw_results) if return_raw else formatted
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}", exc_info=True)
            error_msg = f"[ERROR]\n====\nQuery failed: {str(e)}\n====\n"
            return (error_msg, {'error': str(e)}) if return_raw else error_msg

    def _execute_nodetool_command(self, command, return_raw=False):
        """Executes a nodetool command on a remote node via SSH."""
        if not PARAMIKO_AVAILABLE:
            raise ImportError("Paramiko library is required for nodetool support. Please install it.")

        # Get SSH credentials from settings
        ssh_host = self.settings.get('ssh_host')
        ssh_user = self.settings.get('ssh_user')
        ssh_key_file = self.settings.get('ssh_key_file')

        if not all([ssh_host, ssh_user]):
            raise ConnectionError("Missing SSH configuration (ssh_host, ssh_user) for nodetool.")

        client = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            connect_args = {'hostname': ssh_host, 'username': ssh_user}
            if ssh_key_file:
                connect_args['key_filename'] = ssh_key_file
            
            client.connect(**connect_args)
            
            stdin, stdout, stderr = client.exec_command(f"nodetool {command}")
            
            err = stderr.read().decode().strip()
            if err:
                raise RuntimeError(f"Nodetool command stderr: {err}")
                
            output = stdout.read().decode()
            
            # Parse the text output into structured data
            parsed_data = self._parse_nodetool_output(command, output)
            
            # Format the structured data into an AsciiDoc table
            formatted_output = self._format_results_as_table(parsed_data)
            
            return (formatted_output, parsed_data) if return_raw else formatted_output

        finally:
            if client:
                client.close()

    def _parse_nodetool_output(self, command, output):
        """Dispatcher to parse the text output from different nodetool commands."""
        if command == 'status':
            return self._parse_nodetool_status(output)
        elif command == 'compactionstats':
            return self._parse_nodetool_compactionstats(output)
        elif command == 'tpstats':
            return self._parse_nodetool_tpstats(output)
        else:
            # Fallback for unsupported commands: return raw text
            return [{'command': command, 'output': output}]

    def _parse_nodetool_status(self, output):
        """Parses the output of 'nodetool status' into a list of dicts."""
        nodes = []
        lines = output.strip().split('\n')
        
        # Find the header line that starts with '--'
        header_index = -1
        for i, line in enumerate(lines):
            if line.strip().startswith('--'):
                header_index = i
                break
        
        if header_index == -1:
            return nodes # Could not find header

        # Data lines start right after the header
        data_lines = lines[header_index + 1:]
        
        # Get current datacenter (assumes it appears before the node list)
        current_dc = ''
        for line in lines[:header_index]:
            if "Datacenter:" in line:
                current_dc = line.split("Datacenter:")[1].strip()

        for line in data_lines:
            parts = line.split()
            if len(parts) < 8:
                continue # Skip malformed lines

            nodes.append({
                'datacenter': current_dc,
                'status': parts[0][0], # U or D
                'state': parts[0][1],  # N, L, J, or M
                'address': parts[1],
                'load': ' '.join(parts[2:4]),
                'tokens': int(parts[4]),
                'owns_effective_percent': float(parts[5].replace('%', '')),
                'host_id': parts[6],
                'rack': parts[7]
            })
        return nodes

    def _format_results_as_table(self, results):
        """Builds an AsciiDoc table from a list of dictionaries."""
        if not results:
            return "[NOTE]\n====\nNo results returned.\n====\n"
        
        columns = list(results[0].keys())
        table = ['|===', '|' + '|'.join(columns)]
        for row in results:
            row_values = [str(row.get(col, '')) for col in columns]
            table.append('|' + '|'.join(row_values))
        table.append('|===')
        return '\n'.join(table)

def _parse_nodetool_compactionstats(self, output):
    """Parses the output of 'nodetool compactionstats' into a structured dict."""
    lines = output.strip().split('\n')

    # First, find the pending tasks
    pending_tasks = 0
    for line in lines:
        if "pending tasks" in line.lower():
            try:
                pending_tasks = int(line.split(':')[1].strip())
            except (IndexError, ValueError):
                pass # Keep default if parsing fails

    # Find the header row to identify where the data starts
    header_index = -1
    for i, line in enumerate(lines):
        if "compaction id" in line.lower():
            header_index = i
            break

    active_compactions = []
    if header_index != -1:
        data_lines = lines[header_index + 1:]
        for line in data_lines:
            parts = line.split()
            if len(parts) < 7:
                continue # Skip malformed lines

            active_compactions.append({
                'compaction_id': parts[0],
                'keyspace': parts[1],
                'table': parts[2],
                'completed': float(parts[3]),
                'total': float(parts[4]),
                'unit': parts[5],
                'type': parts[6]
            })

    return {
        'pending_tasks': pending_tasks,
        'active_compactions': active_compactions
    }


def _parse_nodetool_tpstats(self, output):
    """Parses the output of 'nodetool tpstats' into a list of dicts."""
    thread_pools = []
    lines = output.strip().split('\n')

    # Find the header row to identify where the data starts
    header_index = -1
    for i, line in enumerate(lines):
        if "pool name" in line.lower():
            header_index = i
            break

    if header_index == -1:
        return thread_pools # Could not find header

    data_lines = lines[header_index + 1:]
    for line in data_lines:
        parts = line.split()
        if len(parts) < 6:
            continue # Skip malformed or summary lines

        try:
            thread_pools.append({
                'pool_name': parts[0],
                'active': int(parts[1]),
                'pending': int(parts[2]),
                'completed': int(parts[3]),
                'blocked': int(parts[4]),
                'all_time_blocked': int(parts[5])
            })
        except (ValueError, IndexError):
            # This can happen on summary lines or malformed output
            continue

    return thread_pools



