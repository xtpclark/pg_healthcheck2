def get_compaction_history_query(connector):
    """
    Returns the CQL query for recent compaction history.
    
    Args:
        connector: Connector instance (may have version_info)
    
    Returns:
        str: CQL query string
    """
    # Cassandra system table for completed compactions; pending requires JMX/nodetool
    # This provides insight into recent activity as proxy for potential issues
    
    # Version compatibility: columnfamily_name in 3.x, table_name in 4.x+
    if hasattr(connector, 'version_info') and connector.version_info >= (4, 0):
        table_column = 'table_name'
    else:
        table_column = 'columnfamily_name'
    
    return f"""
    SELECT keyspace_name, {table_column} AS columnfamily_name, compaction_time, bytes_in, bytes_out, rows_merged
    FROM system.compaction_history
    ORDER BY compaction_time DESC
    LIMIT 50;
    """