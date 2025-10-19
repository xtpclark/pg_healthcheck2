"""Memory usage queries for Cassandra (shell commands)."""

__all__ = [
    'get_memory_usage_query'
]

import json

def get_memory_usage_query(connector):
    """
    Returns JSON request for memory usage via 'free -m' command.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "free -m"
    })