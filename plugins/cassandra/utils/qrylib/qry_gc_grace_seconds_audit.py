"""GC grace seconds queries for Cassandra."""

__all__ = [
    'get_gc_grace_seconds_query'
]

def get_gc_grace_seconds_query(connector):
    """
    Returns query for table gc_grace_seconds settings.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT keyspace_name, table_name, gc_grace_seconds
    FROM system_schema.tables;
    """
