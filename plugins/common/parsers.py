"""
Parsers for common database tools and shell commands.

Provides structured parsing of output from:
- Cassandra nodetool
- Shell commands (df, ps, free, etc.)
- Custom database tools
"""

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def _parse_size_to_bytes(size_str: str) -> int:
    """
    Converts a size string to bytes.
    
    Examples:
        "108.45 KB" -> 111052
        "1.5 GB" -> 1610612736
        "512 MB" -> 536870912
        "0 bytes" -> 0
    
    Args:
        size_str: Size string with unit (e.g., "108.45 KB")
    
    Returns:
        int: Size in bytes
    """
    if not size_str or size_str.strip() == '0' or 'bytes' in size_str.lower():
        return 0
    
    # Remove commas and extra spaces
    size_str = size_str.replace(',', '').strip()
    
    # Split number and unit
    parts = size_str.split()
    if len(parts) < 2:
        return 0
    
    try:
        number = float(parts[0])
        unit = parts[1].upper()
        
        # Convert to bytes
        multipliers = {
            'B': 1,
            'KB': 1024,
            'MB': 1024 ** 2,
            'GB': 1024 ** 3,
            'TB': 1024 ** 4,
            'KIB': 1024,
            'MIB': 1024 ** 2,
            'GIB': 1024 ** 3,
            'TIB': 1024 ** 4
        }
        
        multiplier = multipliers.get(unit, 1)
        return int(number * multiplier)
    
    except (ValueError, IndexError):
        logger.warning(f"Could not parse size: {size_str}")
        return 0
        
class NodetoolParser:
    """Parses Cassandra nodetool command output into structured data."""
    
    def parse(self, command: str, output: str) -> Any:
        """
        Dispatcher to parse different nodetool commands.
        
        Args:
            command: Nodetool command name (e.g., 'status', 'tpstats')
            output: Raw command output
        
        Returns:
            Parsed structured data (format depends on command)
        """
        parsers = {
            'status': self._parse_status,
            'tpstats': self._parse_tpstats,
            'compactionstats': self._parse_compactionstats,
            'gcstats': self._parse_gcstats,
            'describecluster': self._parse_describecluster,
            'tablestats': self._parse_tablestats,       
            'info': self._parse_info,
            'gossipinfo': self._parse_gossipinfo,    
        }
        
        parser = parsers.get(command)
        if parser:
            return parser(output)
        else:
            logger.warning(f"No parser for nodetool command: {command}")
            return [{'command': command, 'output': output}]
    
    def _parse_status(self, output: str) -> List[Dict]:
        """Parses 'nodetool status' output."""
        nodes = []
        if not output or not output.strip():
            return nodes
        
        lines = output.strip().split('\n')
        current_dc = 'unknown'
        
        for line in lines:
            if "Datacenter:" in line:
                parts = line.split("Datacenter:")
                if len(parts) > 1:
                    current_dc = parts[1].strip()
            
            # Parse node lines
            parts = line.split()
            if len(parts) >= 8 and parts[0] in ('UN', 'UL', 'UJ', 'UM', 'DN', 'DL', 'DJ', 'DM'):
                try:
                    nodes.append({
                        'datacenter': current_dc,
                        'status': parts[0][0],
                        'state': parts[0][1],
                        'address': parts[1],
                        'load': ' '.join(parts[2:4]) if len(parts) > 3 else parts[2],
                        'tokens': int(parts[4]) if parts[4].isdigit() else 0,
                        'owns_effective_percent': float(parts[5].replace('%', '')) if '%' in parts[5] else 0.0,
                        'host_id': parts[6],
                        'rack': parts[7]
                    })
                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse node line: {line} - {e}")
        
        return nodes
    
    def _parse_tpstats(self, output: str) -> List[Dict]:
        """Parses 'nodetool tpstats' output."""
        thread_pools = []
        
        if not output or not output.strip():
            logger.warning("Empty nodetool tpstats output")
            return thread_pools
        
        lines = output.strip().split('\n')

        # Find the header row to identify where the data starts
        header_index = -1
        for i, line in enumerate(lines):
            if "pool name" in line.lower():
                header_index = i
                break

        if header_index == -1:
            logger.warning("Could not find header in nodetool tpstats output")
            return thread_pools

        data_lines = lines[header_index + 1:]
        for line in data_lines:
            parts = line.split()
            if len(parts) < 6:
                continue  # Skip malformed or summary lines

            try:
                thread_pools.append({
                    'pool_name': parts[0],
                    'active': int(parts[1]),
                    'pending': int(parts[2]),
                    'completed': int(parts[3]),
                    'blocked': int(parts[4]),
                    'all_time_blocked': int(parts[5])
                })
            except (ValueError, IndexError) as e:
                # This can happen on summary lines or malformed output
                logger.debug(f"Skipping line in tpstats: {line} - {e}")
                continue

        return thread_pools
    
    def _parse_compactionstats(self, output: str) -> Dict:
        """Parses 'nodetool compactionstats' output."""
        if not output or not output.strip():
            logger.warning("Empty nodetool compactionstats output")
            return {'pending_tasks': 0, 'active_compactions': []}
        
        lines = output.strip().split('\n')

        # First, find the pending tasks
        pending_tasks = 0
        for line in lines:
            if "pending tasks" in line.lower():
                try:
                    parts = line.split(':')
                    if len(parts) > 1:
                        pending_tasks = int(parts[1].strip())
                except (IndexError, ValueError) as e:
                    logger.warning(f"Could not parse pending tasks: {e}")

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
                    continue  # Skip malformed lines

                try:
                    active_compactions.append({
                        'compaction_id': parts[0],
                        'keyspace': parts[1],
                        'table': parts[2],
                        'completed': float(parts[3]),
                        'total': float(parts[4]),
                        'unit': parts[5],
                        'type': parts[6]
                    })
                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse compaction line: {line} - {e}")
                    continue

        return {
            'pending_tasks': pending_tasks,
            'active_compactions': active_compactions
        }
    
    def _parse_gcstats(self, output: str) -> Dict:
        """Parses 'nodetool gcstats' output."""
        if not output or not output.strip():
            logger.warning("Empty nodetool gcstats output")
            return {}
        
        lines = output.strip().split('\n')
        
        # Find the data line (should be after the header line)
        data_line = None
        for i, line in enumerate(lines):
            stripped = line.strip()
            # Skip empty lines and header lines
            if stripped and 'Interval' not in line and 'GC Elapsed' not in line:
                data_line = stripped
                break
        
        if not data_line:
            logger.warning("Could not find data line in gcstats output")
            return {}
        
        try:
            parts = data_line.split()
            
            if len(parts) < 7:
                logger.warning(f"Unexpected gcstats format (expected 7 columns, got {len(parts)})")
                return {}
            
            def safe_int(value, default=0):
                try:
                    if value.upper() == 'NAN':
                        return None
                    return int(value)
                except (ValueError, AttributeError):
                    return default
            
            gc_stats = {
                'interval_ms': safe_int(parts[0]),
                'max_gc_elapsed_ms': safe_int(parts[1]),
                'total_gc_elapsed_ms': safe_int(parts[2]),
                'stdev_gc_elapsed_ms': safe_int(parts[3]),
                'gc_reclaimed_mb': safe_int(parts[4]),
                'collections': safe_int(parts[5]),
                'direct_memory_bytes': safe_int(parts[6], default=-1)
            }
            
            return gc_stats
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Failed to parse gcstats line: {data_line} - {e}")
            return {}

    def _parse_describecluster(self, output: str) -> Dict:
        """
        Parses 'nodetool describecluster' output.
        
        Example output:
            Cluster Information:
                Name: Test Cluster
                Snitch: org.apache.cassandra.locator.SimpleSnitch
                Schema versions:
                    SCHEMA_UUID: [192.168.1.10, 192.168.1.11]
                    UNREACHABLE: [192.168.1.12]
        
        Returns:
            dict: Cluster information including schema versions
        """
        if not output or not output.strip():
            logger.warning("Empty nodetool describecluster output")
            return {'schema_versions': []}
        
        lines = output.strip().split('\n')
        
        cluster_info = {
            'name': 'Unknown',
            'snitch': 'Unknown',
            'partitioner': 'Unknown',
            'schema_versions': []
        }
        
        current_section = None
        schema_section_started = False
        
        for line in lines:
            stripped_line = line.strip()
            
            # Parse basic cluster info
            if 'Name:' in line and 'Cluster Information' not in line:
                cluster_info['name'] = stripped_line.split(':', 1)[1].strip()
            elif 'Snitch:' in line:
                cluster_info['snitch'] = stripped_line.split(':', 1)[1].strip()
            elif 'Partitioner:' in line:
                cluster_info['partitioner'] = stripped_line.split(':', 1)[1].strip()
            
            # Detect schema versions section
            elif 'Schema versions:' in line:
                current_section = 'schema_versions'
                schema_section_started = True
                continue
            
            # End schema versions section when we hit other sections
            elif schema_section_started and any(keyword in line for keyword in [
                'Stats for all nodes', 'Data Centers', 'Database versions', 
                'Keyspaces', 'Live', 'Joining', 'Moving', 'Leaving'
            ]):
                schema_section_started = False
                continue
            
            # Parse schema versions (only UUID-like strings or UNREACHABLE)
            elif schema_section_started and ':' in stripped_line:
                parts = stripped_line.split(':', 1)
                if len(parts) == 2:
                    version = parts[0].strip()
                    endpoints_str = parts[1].strip()
                    
                    # Only process if version looks like a UUID or is UNREACHABLE
                    # UUIDs are 36 characters with hyphens: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
                    is_uuid = (len(version) == 36 and version.count('-') == 4)
                    is_unreachable = version.upper() == 'UNREACHABLE'
                    
                    if is_uuid or is_unreachable:
                        # Remove brackets and split
                        endpoints_str = endpoints_str.strip('[]')
                        endpoints = [ep.strip() for ep in endpoints_str.split(',') if ep.strip()]
                        
                        cluster_info['schema_versions'].append({
                            'version': version,
                            'endpoints': endpoints
                        })
        
        return cluster_info
    
    def old_parse_describecluster(self, output: str) -> Dict:
        """
        Parses 'nodetool describecluster' output.
        
        Example output:
            Cluster Information:
                Name: Test Cluster
                Snitch: org.apache.cassandra.locator.SimpleSnitch
                DynamicEndPointSnitch: enabled
                Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
                Schema versions:
                    SCHEMA_VERSION_1: [192.168.1.10]
        
        Returns:
            dict: Cluster information including schema versions
        """
        if not output or not output.strip():
            logger.warning("Empty nodetool describecluster output")
            return {'schema_versions': []}
        
        lines = output.strip().split('\n')
        
        cluster_info = {
            'name': 'Unknown',
            'snitch': 'Unknown',
            'partitioner': 'Unknown',
            'schema_versions': []
        }
        
        current_section = None
        
        for line in lines:
            line = line.strip()
            
            # Parse basic cluster info
            if line.startswith('Name:'):
                cluster_info['name'] = line.split(':', 1)[1].strip()
            elif line.startswith('Snitch:'):
                cluster_info['snitch'] = line.split(':', 1)[1].strip()
            elif line.startswith('Partitioner:'):
                cluster_info['partitioner'] = line.split(':', 1)[1].strip()
            
            # Detect schema versions section
            elif 'Schema versions:' in line:
                current_section = 'schema_versions'
                continue
            
            # Parse schema versions
            elif current_section == 'schema_versions' and ':' in line:
                # Format: "SCHEMA_UUID: [ip1, ip2, ip3]"
                parts = line.split(':', 1)
                if len(parts) == 2:
                    version = parts[0].strip()
                    endpoints_str = parts[1].strip()
                    
                    # Parse endpoint list
                    # Remove brackets and split by comma
                    endpoints_str = endpoints_str.strip('[]')
                    endpoints = [ep.strip() for ep in endpoints_str.split(',') if ep.strip()]
                    
                    cluster_info['schema_versions'].append({
                        'version': version,
                        'endpoints': endpoints
                    })
        
        return cluster_info

    def _parse_info(self, output: str) -> Dict:
        """
        Parses 'nodetool info' output.
        
        Example output:
            ID                     : aaa-bbb-ccc-ddd
            Gossip active          : true
            Thrift active          : false
            Native Transport active: true
            Load                   : 108.45 KB
            Generation No          : 1234567890
            Uptime (seconds)       : 86400
            Heap Memory (MB)       : 512.00 / 2048.00
            Off Heap Memory (MB)   : 256.00
            Data Center            : datacenter1
            Rack                   : rack1
            Exceptions             : 0
            Key Cache              : entries 100, size 1.5 KB, capacity 50 MB, 95 hits, 100 requests, 0.950 recent hit rate, 14400 save period in seconds
            Row Cache              : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
            Counter Cache          : entries 0, size 0 bytes, capacity 25 MB, 0 hits, 0 requests, NaN recent hit rate, 7200 save period in seconds
            Percent Repaired       : 100.0%
            Token                  : (invoke with -T/--tokens to see all 256 tokens)
        
        Returns:
            dict: Node information including load, uptime, memory, etc.
        """
        if not output or not output.strip():
            logger.warning("Empty nodetool info output")
            return {}
        
        info = {}
        lines = output.strip().split('\n')
        
        for line in lines:
            if ':' not in line:
                continue
            
            # Split on first colon only
            parts = line.split(':', 1)
            if len(parts) != 2:
                continue
            
            key = parts[0].strip()
            value = parts[1].strip()
            
            # Parse specific fields
            if key == 'ID':
                info['id'] = value
            elif key == 'Gossip active':
                info['gossip_active'] = value.lower() == 'true'
            elif key == 'Thrift active':
                info['thrift_active'] = value.lower() == 'true'
            elif key == 'Native Transport active':
                info['native_transport_active'] = value.lower() == 'true'
            elif key == 'Load':
                info['load'] = value
                info['load_bytes'] = _parse_size_to_bytes(value)
            elif key == 'Generation No':
                info['generation_no'] = int(value) if value.isdigit() else value
            elif key == 'Uptime (seconds)':
                info['uptime_seconds'] = int(value) if value.isdigit() else 0
            elif key == 'Heap Memory (MB)':
                # Format: "512.00 / 2048.00"
                if '/' in value:
                    used, total = value.split('/')
                    info['heap_memory_mb_used'] = float(used.strip())
                    info['heap_memory_mb_total'] = float(total.strip())
                    info['heap_memory_percent'] = (
                        info['heap_memory_mb_used'] / info['heap_memory_mb_total'] * 100
                        if info['heap_memory_mb_total'] > 0 else 0
                    )
            elif key == 'Off Heap Memory (MB)':
                info['off_heap_memory_mb'] = float(value)
            elif key == 'Data Center':
                info['datacenter'] = value
            elif key == 'Rack':
                info['rack'] = value
            elif key == 'Exceptions':
                info['exceptions'] = int(value) if value.isdigit() else 0
            elif key == 'Percent Repaired':
                # Remove % sign and convert to float
                info['percent_repaired'] = float(value.rstrip('%'))
            elif 'Cache' in key:
                # Parse cache information (Key Cache, Row Cache, Counter Cache)
                cache_name = key.lower().replace(' ', '_')
                info[cache_name] = value
        
        return info

    def _parse_gossipinfo(self, output: str) -> Dict:
        """
        Parses 'nodetool gossipinfo' output.
        
        Example output:
            /192.168.1.10
              generation:1234567890
              heartbeat:98765
              STATUS:NORMAL,-9223372036854775808
              LOAD:108.45KB
              SCHEMA:909ab78a-408f-34a2-872b-4ca50d2dfe2a
              DC:datacenter1
              RACK:rack1
              RELEASE_VERSION:4.1.10
              INTERNAL_IP:192.168.1.10
              RPC_ADDRESS:192.168.1.10
              SEVERITY:0.0
              NET_VERSION:12
              HOST_ID:aaa-bbb-ccc-ddd
              TOKENS:<hidden>
            /192.168.1.11
              generation:1234567891
              heartbeat:98766
              ...
        
        Returns:
            dict: Map of node IP to gossip state information
        """
        if not output or not output.strip():
            logger.warning("Empty nodetool gossipinfo output")
            return {}
        
        gossip_states = {}
        current_node = None
        
        lines = output.strip().split('\n')
        
        for line in lines:
            stripped_line = line.strip()
            
            # Check if this is a node IP line
            if stripped_line.startswith('/'):
                current_node = stripped_line.lstrip('/')
                gossip_states[current_node] = {}
            elif current_node and ':' in stripped_line:
                # Parse key:value pairs
                parts = stripped_line.split(':', 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip()
                    
                    # Convert to lowercase with underscores for consistency
                    key_normalized = key.lower().replace('-', '_')
                    
                    # Parse specific values
                    if key in ('generation', 'heartbeat'):
                        gossip_states[current_node][key_normalized] = int(value) if value.isdigit() else value
                    elif key == 'STATUS':
                        # Format: "NORMAL,-9223372036854775808" or "LEAVING,token"
                        status_parts = value.split(',')
                        gossip_states[current_node]['status'] = status_parts[0]
                        if len(status_parts) > 1:
                            gossip_states[current_node]['status_token'] = status_parts[1]
                    elif key == 'LOAD':
                        gossip_states[current_node]['load'] = value
                        gossip_states[current_node]['load_bytes'] = _parse_size_to_bytes(value)
                    elif key == 'SEVERITY':
                        gossip_states[current_node]['severity'] = float(value)
                    else:
                        gossip_states[current_node][key_normalized] = value
        
        return gossip_states



    
    def _parse_tablestats(self, output: str) -> List[Dict]:
        """
        Parses 'nodetool tablestats' output.
        
        Example output:
            Keyspace : system_auth
                Read Count: 0
                Read Latency: NaN ms
                Write Count: 0
                Write Latency: NaN ms
                Pending Flushes: 0
                    Table: roles
                    SSTable count: 1
                    Space used (live): 12345
                    Space used (total): 12345
                    ...
        
        Returns:
            list[dict]: One dict per table with keyspace, table name, and statistics
        """
        if not output or not output.strip():
            logger.warning("Empty nodetool tablestats output")
            return []
        
        lines = output.strip().split('\n')
        tables = []
        current_keyspace = None
        current_table = None
        current_table_data = {}
        
        for line in lines:
            # Detect keyspace
            if line.startswith('Keyspace'):
                # Save previous table if exists
                if current_table and current_table_data:
                    tables.append(current_table_data)
                    current_table_data = {}
                
                # Parse keyspace name
                parts = line.split(':', 1)
                if len(parts) == 2:
                    current_keyspace = parts[1].strip()
                continue
            
            # Detect table
            if 'Table:' in line or 'Table (index):' in line:
                # Save previous table if exists
                if current_table and current_table_data:
                    tables.append(current_table_data)
                
                # Parse table name
                if 'Table:' in line:
                    current_table = line.split('Table:', 1)[1].strip()
                elif 'Table (index):' in line:
                    current_table = line.split('Table (index):', 1)[1].strip()
                
                # Initialize new table data
                current_table_data = {
                    'keyspace': current_keyspace,
                    'table': current_table
                }
                continue
            
            # Parse table statistics
            if current_table and ':' in line:
                line = line.strip()
                parts = line.split(':', 1)
                if len(parts) == 2:
                    key = parts[0].strip().lower().replace(' ', '_')
                    value = parts[1].strip()
                    
                    # Special handling for space used
                    if 'space used (live)' in line.lower():
                        current_table_data['space_used_live'] = value
                    elif 'space used (total)' in line.lower():
                        current_table_data['space_used_total'] = value
                    elif 'sstable count' in line.lower():
                        try:
                            current_table_data['sstable_count'] = int(value)
                        except ValueError:
                            current_table_data['sstable_count'] = value
                    else:
                        # Store other metrics generically
                        current_table_data[key] = value
        
        # Don't forget the last table
        if current_table and current_table_data:
            tables.append(current_table_data)
        
        return tables

class ShellCommandParser:
    """Parses common shell command output into structured data."""
    
    @staticmethod
    def parse_df(output: str) -> List[Dict]:
        """
        Parses 'df -h' output into structured data.
        
        Returns:
            List of filesystem info dicts
        """
        filesystems = []
        lines = output.strip().split('\n')
        
        if len(lines) < 2:
            return filesystems
        
        # Skip header
        for line in lines[1:]:
            parts = line.split()
            if len(parts) >= 6:
                filesystems.append({
                    'filesystem': parts[0],
                    'size': parts[1],
                    'used': parts[2],
                    'avail': parts[3],
                    'use_pct': parts[4],
                    'mounted_on': parts[5]
                })
        
        return filesystems
    
    @staticmethod
    def parse_free(output: str) -> Dict:
        """
        Parses 'free -m' output into structured data.
        
        Returns:
            Memory info dict
        """
        memory_info = {}
        lines = output.strip().split('\n')
        
        for line in lines:
            if line.startswith('Mem:'):
                parts = line.split()
                if len(parts) >= 4:
                    memory_info = {
                        'total': parts[1],
                        'used': parts[2],
                        'free': parts[3],
                        'shared': parts[4] if len(parts) > 4 else '0',
                        'buffers': parts[5] if len(parts) > 5 else '0',
                        'available': parts[6] if len(parts) > 6 else parts[3]
                    }
                break
        
        return memory_info
