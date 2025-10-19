"""UDF and aggregates queries for Cassandra."""

__all__ = [
    'get_functions_query',
    'get_aggregates_query'
]

def get_functions_query(connector):
    """
    Returns query for user-defined functions.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT keyspace_name, function_name, language, return_type
    FROM system_schema.functions;
    """

def get_aggregates_query(connector):
    """
    Returns query for user-defined aggregates.
    
    Args:
        connector: Cassandra connector instance
    
    Returns:
        str: CQL SELECT statement
    """
    return """
    SELECT keyspace_name, aggregate_name AS function_name, return_type
    FROM system_schema.aggregates;
    """