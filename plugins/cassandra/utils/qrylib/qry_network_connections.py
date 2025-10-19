"""Network connection queries for Cassandra (shell commands)."""

__all__ = [
    'get_network_connections_query'
]

import json


def get_network_connections_query(connector):
    """
    Returns JSON request for network statistics via 'netstat -s' command.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "netstat -s || ss -s"
    })