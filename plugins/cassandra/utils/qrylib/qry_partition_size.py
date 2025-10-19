"""Partition size queries for Cassandra."""

__all__ = [
    'get_partition_size_query'
]

def get_partition_size_query(connector):
    """
    Returns query for table metrics including max_partition_size.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT keyspace_name, table_name, max_partition_size
    FROM system_views.table_metrics;
    """