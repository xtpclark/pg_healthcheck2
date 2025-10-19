"""Table compression queries for Cassandra."""

__all__ = [
    'get_table_compression_query'
]

def get_table_compression_query(connector):
    """
    Returns query for table compression settings.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT keyspace_name, table_name, compression
    FROM system_schema.tables;
    """