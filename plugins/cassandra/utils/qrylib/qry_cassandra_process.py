"""Cassandra process queries for shell commands."""

__all__ = [
    'get_cassandra_process_query'
]

import json

def get_cassandra_process_query(connector):
    """
    Returns JSON request for checking Cassandra process via 'ps aux' command.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "shell",
        "command": "ps aux | grep cassandra | grep -v grep"
    })