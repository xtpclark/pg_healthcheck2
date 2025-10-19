"""Keyspace replication queries for Cassandra health check."""

__all__ = [
    'get_keyspace_replication_health_query'
]

def get_keyspace_replication_health_query(connector):
    """
    Returns query for keyspace replication strategies.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT keyspace_name, replication, durable_writes
    FROM system_schema.keyspaces;
    """