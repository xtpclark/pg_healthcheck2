def get_pg_stat_statements_query(connector, query_type='standard'):
    """
    Get pg_stat_statements query based on PostgreSQL version.
    
    Args:
        connector (PostgresConnector): The active database connector instance.
        query_type (str): Type of query ('standard', 'write_activity', 'function_performance')
    
    Returns:
        str: SQL query appropriate for the PostgreSQL version
    """
    compatibility = connector.version_info
    
    if query_type == 'standard':
        if compatibility.get('is_pg14_or_newer'):
            return """
                SELECT query, calls, total_exec_time, mean_exec_time, rows
                FROM pg_stat_statements
                WHERE calls > 0
                ORDER BY total_exec_time DESC
            """
        else:
            return """
                SELECT query, calls, total_time, mean_time, rows
                FROM pg_stat_statements
                WHERE calls > 0
                ORDER BY total_time DESC
            """
    
    elif query_type == 'write_activity':
        if compatibility.get('is_pg14_or_newer'):
            return """
                SELECT query, calls, total_exec_time, mean_exec_time, rows,
                       shared_blks_written, local_blks_written, temp_blks_written, wal_bytes
                FROM pg_stat_statements
                ORDER BY rows DESC, wal_bytes DESC
            """
        else:
            return """
                SELECT query, calls, total_time, mean_time, rows,
                       temp_blks_written
                FROM pg_stat_statements
                ORDER BY rows DESC
            """
    
    elif query_type == 'function_performance':
        if compatibility.get('is_pg14_or_newer'):
            return """
                SELECT query, calls, total_exec_time, mean_exec_time
                FROM pg_stat_statements
                ORDER BY total_exec_time DESC
            """
        else:
            return """
                SELECT f.funcid::regproc AS function_name,
                       s.calls, s.total_time, s.self_time
                FROM pg_stat_statements s
                JOIN pg_proc f ON s.funcid = f.oid
                ORDER BY s.total_time DESC
            """
    
    else:
        return get_pg_stat_statements_query(connector, 'standard')
