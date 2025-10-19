"""GC stats queries for Cassandra (nodetool)."""

__all__ = [
    'get_gcstats_query'
]

import json

def get_gcstats_query(connector):
    """
    Returns JSON request for 'nodetool gcstats' command.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "nodetool",
        "command": "gcstats"
    })