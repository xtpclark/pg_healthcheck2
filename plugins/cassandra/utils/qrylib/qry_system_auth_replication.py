"""Queries for system_auth replication check in Cassandra."""

__all__ = [
    'get_local_dc_query',
    'get_peers_query',
    'get_system_auth_replication_query'
]

def get_local_dc_query(connector):
    """
    Returns query for local datacenter from system.local.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT data_center
    FROM system.local;
    """

def get_peers_query(connector):
    """
    Returns version-aware query for datacenters from peers.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    table = 'system.peers_v2'
    if hasattr(connector, 'version_info'):
        major = connector.version_info.get('major_version', 3)
        if major < 4:
            table = 'system.peers'
    return f"""
    SELECT data_center
    FROM {table};
    """

def get_system_auth_replication_query(connector):
    """
    Returns query for system_auth keyspace replication.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT keyspace_name, replication, durable_writes
    FROM system_schema.keyspaces
    WHERE keyspace_name = 'system_auth';
    """
