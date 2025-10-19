"""Row cache queries for Cassandra."""

__all__ = [
    'get_row_cache_query'
]

def get_row_cache_query(connector):
    """
    Returns query for table caching settings.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT keyspace_name, table_name, caching
    FROM system_schema.tables;
    """