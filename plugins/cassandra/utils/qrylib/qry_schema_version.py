"""Nodetool queries for schema version consistency checks."""

import json

__all__ = [
    'get_nodetool_describecluster_query'
]

def get_nodetool_describecluster_query(connector):
    """
    Returns JSON request for 'nodetool describecluster' command.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "nodetool",
        "command": "describecluster"
    })
