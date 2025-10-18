"""Commitlog directory size queries for Cassandra (shell commands)."""

import json

__all__ = [
    'get_commitlog_directory_size_query'
]

def get_commitlog_directory_size_query(connector, commitlog_dir='/var/lib/cassandra/commitlog'):
    """
    Returns JSON request for Cassandra commitlog directory size via 'du -sh' command.
    
    Args:
        connector: Cassandra connector instance
        commitlog_dir: Path to Cassandra commitlog directory (default: /var/lib/cassandra/commitlog)
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": f"du -sh {commitlog_dir}"
    })