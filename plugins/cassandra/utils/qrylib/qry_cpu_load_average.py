"""CPU load average queries for Cassandra (shell commands)."""

__all__ = [
    'get_cpu_load_average_query'
]

import json

def get_cpu_load_average_query(connector):
    """
    Returns JSON request for 'uptime' command to get load averages.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "uptime"
    })