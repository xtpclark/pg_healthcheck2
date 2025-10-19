import json

"""Nodetool queries for compaction pending tasks in Cassandra."""

__all__ = [
    'get_nodetool_compactionstats_query'
]

def get_nodetool_compactionstats_query(connector):
    """
    Returns JSON request for 'nodetool compactionstats' command.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "nodetool",
        "command": "compactionstats"
    })
