"""Materialized views queries for Cassandra."""

__all__ = [
    'get_materialized_views_query'
]

def get_materialized_views_query(connector):
    """
    Returns query for all materialized views.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT keyspace_name, view_name, base_table_name, where_clause
    FROM system_schema.views;
    """