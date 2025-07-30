def get_top_write_queries_query(connector):
    """
    Returns a version-aware query to find top write-intensive queries
    from pg_stat_statements.
    """
    # Sanitize query text for safe AsciiDoc table display
    query_select_prefix = "REPLACE(REPLACE(LEFT(query, 150), E'\\n', ' '), '|', ' ') || '...' AS query"

    if connector.version_info.get('is_pg14_or_newer'):
        return f"""
            SELECT {query_select_prefix}, calls, total_exec_time, mean_exec_time, rows,
                   shared_blks_written, local_blks_written, temp_blks_written, wal_bytes
            FROM pg_stat_statements
            ORDER BY wal_bytes DESC, shared_blks_written DESC
            LIMIT %(limit)s;
        """
    else:
        # For older versions, we rely on blocks written as the primary indicator
        return f"""
            SELECT {query_select_prefix}, calls, total_time AS total_exec_time, mean_time AS mean_exec_time, rows,
                   shared_blks_written, local_blks_written, temp_blks_written
            FROM pg_stat_statements
            ORDER BY shared_blks_written DESC, rows DESC
            LIMIT %(limit)s;
        """
