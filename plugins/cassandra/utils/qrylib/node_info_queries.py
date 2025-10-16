def get_node_info_query(connector):
    """
    Returns the query for retrieving basic node information from system.local.
    
    Args:
        connector: Connector instance (may have version_info)
    
    Returns:
        str: CQL query string
    """
    # system.local is consistent across versions, no version-specific logic needed
    return """
    SELECT cluster_name, data_center, rack, release_version, cql_version,
           native_protocol_version, host_id, listen_address, broadcast_address,
           rpc_address, partitioner
    FROM system.local;
    """