"""Nodetool queries for disk space analysis using tablestats."""

import json

__all__ = [
    'get_nodetool_tablestats_query'
]


def get_nodetool_tablestats_query(connector):
    """
    Returns JSON request for 'nodetool tablestats' command to get all tables.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "nodetool",
        "command": "tablestats"
    })
