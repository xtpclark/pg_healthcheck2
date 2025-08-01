"""
Query library for the hot_queries check.
"""

def get_hot_queries_query(connector):
    """
    Returns a version-aware query to find "hot" queries based on shared
    buffer hits from the pg_stat_statements extension.
    """
    version_info = connector.version_info

    # Determine the correct column name for execution time based on PG version
    time_column = 'total_exec_time' if version_info.get('is_pg14_or_newer') else 'total_time'
    mean_time_column = 'mean_exec_time' if version_info.get('is_pg14_or_newer') else 'mean_time'

    # The `limit` parameter is supplied by the calling check module.
    return f"""
        SELECT
            substring(regexp_replace(query, '\\s+', ' ', 'g') for 120) AS query, -- MODIFIED
            -- query,
            calls,
            {time_column},
            {mean_time_column},
            rows,
            shared_blks_hit,
            shared_blks_read
        FROM pg_stat_statements
        WHERE calls > 0
        ORDER BY shared_blks_hit DESC
        LIMIT %(limit)s;
    """
