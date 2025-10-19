"""Nodetool gossipinfo queries for Cassandra."""

__all__ = [
    'get_nodetool_gossipinfo_query'
]

import json

def get_nodetool_gossipinfo_query(connector):
    """
    Returns JSON request for 'nodetool gossipinfo' command.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "nodetool",
        "command": "gossipinfo"
    })