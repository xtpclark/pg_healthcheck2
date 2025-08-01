"""
Query library for the consolidated deep_query_analysis check.
"""

def _get_query_base(connector, order_by_clause):
    """
    Internal helper function to generate the base query with version-aware columns.
    It consistently aliases columns to 'total_time' and 'mean_time'.
    """
    compatibility = connector.version_info
    
    trimmed_query = "substring(regexp_replace(query, '\\s+', ' ', 'g') for 120) AS query"
    total_time_col = 'total_exec_time' if compatibility.get('is_pg14_or_newer') else 'total_time'
    mean_time_col = 'mean_exec_time' if compatibility.get('is_pg14_or_newer') else 'mean_time'
    
    return f"""
        SELECT {trimmed_query}, calls, 
               {total_time_col} AS total_time, 
               {mean_time_col} AS mean_time,
               rows
        FROM pg_stat_statements
        WHERE calls > 0
        ORDER BY {order_by_clause}
    """

def get_queries_by_total_time_query(connector):
    """Returns query for top queries by total execution time."""
    return _get_query_base(connector, 'total_time DESC')

def get_queries_by_mean_time_query(connector):
    """Returns query for top queries by mean execution time."""
    return _get_query_base(connector, 'mean_time DESC')
    
def get_queries_by_calls_query(connector):
    """Returns query for top queries by call count."""
    return _get_query_base(connector, 'calls DESC')

def get_hot_queries_query(connector):
    """Returns query for 'hot' queries by shared buffer hits."""
    base_query = _get_query_base(connector, 'shared_blks_hit DESC')
    # Add the shared_blks_hit column to the output
    return base_query.replace("rows", "rows, shared_blks_hit")

def get_write_intensive_queries_query(connector):
    """Returns query for write-intensive queries."""
    trimmed_query = "substring(regexp_replace(query, '\\s+', ' ', 'g') for 120) AS query"
    if connector.version_info.get('is_pg14_or_newer'):
        return f"""
            SELECT {trimmed_query}, calls, total_exec_time, mean_exec_time, rows,
                   shared_blks_written, temp_blks_written, wal_bytes
            FROM pg_stat_statements
            WHERE shared_blks_written > 0 OR temp_blks_written > 0 OR wal_bytes > 0
            ORDER BY wal_bytes DESC, shared_blks_written DESC
        """
    else:
        # Older versions have less detailed stats
        return f"""
            SELECT {trimmed_query}, calls, total_time, mean_time, rows,
                   shared_blks_written, temp_blks_written
            FROM pg_stat_statements
            WHERE shared_blks_written > 0 OR temp_blks_written > 0
            ORDER BY shared_blks_written DESC
        """
