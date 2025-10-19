"""Schema version consistency queries for Cassandra."""

__all__ = [
    'get_local_schema_version_query',
    'get_peers_schema_version_query'
]


def get_local_schema_version_query(connector):
    """
    Returns query for local schema version.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return "SELECT schema_version FROM system.local;"


def get_peers_schema_version_query(connector):
    """
    Returns version-aware query for peers schema versions.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement (peers or peers_v2 based on version)
    """
    if hasattr(connector, 'version_info'):
        major = connector.version_info.get('major_version', 3)
        if major >= 4:
            return "SELECT schema_version FROM system.peers_v2;"
        else:
            return "SELECT schema_version FROM system.peers;"
    # Default to 3.x
    return "SELECT schema_version FROM system.peers;"
