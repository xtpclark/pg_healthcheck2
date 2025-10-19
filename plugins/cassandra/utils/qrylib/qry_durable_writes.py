"""Durable writes queries for Cassandra."""

__all__ = [
    'get_durable_writes_query'
]

def get_durable_writes_query(connector):
    """
    Returns query for keyspace durable_writes settings.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT keyspace_name, durable_writes
    FROM system_schema.keyspaces;
    """