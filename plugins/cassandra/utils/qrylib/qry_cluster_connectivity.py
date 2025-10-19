"""Cluster connectivity queries for Cassandra (CQL)."""

__all__ = [
    'get_local_query',
    'get_peers_query'
]

def get_local_query(connector):
    """
    Returns query for local node information.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT cluster_name, data_center, rack, release_version,
           listen_address, broadcast_address, rpc_address
    FROM system.local;
    """

def get_peers_query(connector):
    """
    Returns version-aware query for peer information.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    if hasattr(connector, 'version_info'):
        major = connector.version_info.get('major_version', 0)
        if major >= 4:
            return """
            SELECT peer, data_center, rack, release_version
            FROM system.peers_v2;
            """
        else:
            return """
            SELECT peer, data_center, rack, release_version
            FROM system.peers;
            """
    # Default to 4.x
    return """
    SELECT peer, data_center, rack, release_version
    FROM system.peers_v2;
    """