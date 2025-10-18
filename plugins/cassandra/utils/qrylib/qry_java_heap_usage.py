"""Java heap usage queries for Cassandra (nodetool)."""

__all__ = [
    'get_java_heap_usage_query'
]

import json


def get_java_heap_usage_query(connector):
    """
    Returns JSON request for 'nodetool info' command to get heap usage.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "nodetool",
        "command": "info"
    })