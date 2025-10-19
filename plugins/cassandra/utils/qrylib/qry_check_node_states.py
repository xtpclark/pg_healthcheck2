"""Node states queries for Cassandra (nodetool status)."""

import json

__all__ = [
    'get_node_states_query'
]

def get_node_states_query(connector):
    """
    Returns JSON request for 'nodetool status' command to verify node states.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: JSON string with operation and command
    """
    return json.dumps({
        "operation": "nodetool",
        "command": "status"
    })