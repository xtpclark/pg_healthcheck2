"""Data directory disk space queries for Cassandra (shell commands)."""

import json

__all__ = [
    'get_data_directory_disk_space_query'
]

def get_data_directory_disk_space_query(connector, data_dir='/var/lib/cassandra'):
    """
    Returns JSON request for disk space check on Cassandra data directory via 'df -h'.
    
    Args:
        connector: Cassandra connector instance
        data_dir: Path to Cassandra data directory (default: /var/lib/cassandra)
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": f"df -h {data_dir}"
    })