
"""
Query library for the query_analysis check.
"""

def get_query_workload_summary_query():
    """Returns the query for a high-level summary of pg_stat_statements."""
    # This query is version-agnostic.
    # PG14+ uses total_exec_time, but for a simple sum, total_time is fine as a fallback name.
    time_column = 'total_exec_time' # A reasonable default for modern versions
    return f"""
        SELECT
            COUNT(*) as total_queries_tracked,
            SUM(calls) as total_calls,
            SUM({time_column}) as total_execution_time_all_queries_ms,
            SUM(rows) as total_rows_processed
        FROM pg_stat_statements;
    """

def get_top_queries_by_metric_query(connector, order_by_metric='time'):
    """
    Returns a version-aware query to find top queries from pg_stat_statements,
    ordered by a specified metric.
    """
    version_info = connector.version_info
    time_column = 'total_exec_time' if version_info.get('is_pg14_or_newer') else 'total_time'
    
    order_by_clause = 'calls DESC' if order_by_metric == 'calls' else f'{time_column} DESC'

    return f"""
        SELECT 
	-- query, 
        substring(regexp_replace(query, '\\s+', ' ', 'g') for 120) AS query, -- MODIFIED
	calls, {time_column}, rows
        FROM pg_stat_statements
        WHERE calls > 0
        ORDER BY {order_by_clause}
    """

def get_top_queries_by_io_time_query(connector):
    """
    Returns a query for top queries by I/O time, supporting multiple generations
    of pg_stat_statements column names.
    """

    # Define the trimmed query text once to use in all branches
    trimmed_query = "substring(regexp_replace(query, '\\s+', ' ', 'g') for 120) AS query"

    if connector.has_pgstat_new_io_time:
        # PG17+ style
        return f"""
            SELECT (COALESCE(shared_blk_read_time, 0) + COALESCE(shared_blk_write_time, 0) +
                    COALESCE(local_blk_read_time, 0) + COALESCE(local_blk_write_time, 0) +
                    COALESCE(temp_blk_read_time, 0) + COALESCE(temp_blk_write_time, 0)) as total_io_time,
                   calls, {trimmed_query}
            FROM pg_stat_statements ORDER BY total_io_time DESC
        """
    elif connector.has_pgstat_legacy_io_time:
        # PG13-16 style
        return f"""
            SELECT (blk_read_time + blk_write_time) as total_io_time, calls, {trimmed_query}
            FROM pg_stat_statements ORDER BY total_io_time DESC
        """
    else:
        # Fallback for older versions
        time_column = 'total_exec_time' if connector.version_info.get('is_pg14_or_newer') else 'total_time'
        return f"""
            SELECT {time_column} as total_exec_time_as_proxy_for_io, calls , {trimmed_query}
            FROM pg_stat_statements ORDER BY {time_column} DESC
        """
 
