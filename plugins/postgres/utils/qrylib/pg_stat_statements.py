def get_pg_stat_statements_query(connector, query_type='standard', order_by='total_time'):
    """
    Get pg_stat_statements query based on PostgreSQL version.
    
    Args:
        connector (PostgresConnector): The active database connector instance.
        query_type (str): Type of query.
        order_by (str): The metric to order by ('total_time' or 'mean_time').
    """
    compatibility = connector.version_info
    # Define the trimmed query text, aliasing it back to 'query' for compatibility with rules
    trimmed_query_column = "substring(regexp_replace(query, '\\s+', ' ', 'g') for 120) AS query"

    if query_type == 'standard':
        total_time_col = 'total_exec_time' if compatibility.get('is_pg14_or_newer') else 'total_time'
        mean_time_col = 'mean_exec_time' if compatibility.get('is_pg14_or_newer') else 'mean_time'
        order_by_col = mean_time_col if order_by == 'mean_time' else total_time_col
        # Return a single f-string with all columns aliased for consistency
        return f"""
            SELECT {trimmed_query_column}, calls,
                   {total_time_col} AS total_time,
                   {mean_time_col} AS mean_time,
                   rows
            FROM pg_stat_statements
            WHERE calls > 0
            ORDER BY {order_by_col} DESC
        """

    elif query_type == 'write_activity':
        if compatibility.get('is_pg14_or_newer'):
            return f"""
                SELECT {trimmed_query_column}, calls, total_exec_time, mean_exec_time, rows,
                       shared_blks_written, local_blks_written, temp_blks_written, wal_bytes
                FROM pg_stat_statements
                ORDER BY rows DESC, wal_bytes DESC
            """
        else:
            return f"""
                SELECT {trimmed_query_column}, calls, total_time, mean_time, rows,
                       temp_blks_written
                FROM pg_stat_statements
                ORDER BY rows DESC
            """

    elif query_type == 'function_performance':
        if compatibility.get('is_pg14_or_newer'):
            return f"""
                SELECT {trimmed_query_column}, calls, total_exec_time, mean_exec_time
                FROM pg_stat_statements
                ORDER BY total_exec_time DESC
            """
        else:
            # This branch does not select the 'query' column, so it remains a regular string
            return """
                SELECT f.funcid::regproc AS function_name,
                       s.calls, s.total_time, s.self_time
                FROM pg_stat_statements s
                JOIN pg_proc f ON s.funcid = f.oid
                ORDER BY s.total_time DESC
            """

    else:
        return get_pg_stat_statements_query(connector, 'standard', order_by=order_by)
