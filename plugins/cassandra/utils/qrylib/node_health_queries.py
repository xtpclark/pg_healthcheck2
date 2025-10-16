def get_local_node_query(connector):
    """
    Returns the CQL query for local node information.
    
    Args:
        connector: Connector instance (may have version_info)
    
    Returns:
        str: CQL query string
    """
    # Cassandra system.local provides node metadata
    return """
    SELECT host_id, tokens, address
    FROM system.local;
    """


def get_peers_query(connector):
    """
    Returns the CQL query for peer nodes (gossip health).
    
    Args:
        connector: Connector instance
    
    Returns:
        str: CQL query string
    """
    # system.peers shows connected peers
    return """
    SELECT peer, host_id, rpc_address, schema_version, tokens
    FROM system.peers;
    """