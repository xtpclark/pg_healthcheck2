def get_top_queries_by_io_time_query(connector):
    """
    Returns a query for top queries by I/O time, supporting multiple generations
    of pg_stat_statements column names.
    """
    if connector.has_pgstat_new_io_time:
        # PG17+ style: Sums up shared, local, and temp I/O times
        return """
            SELECT
                (COALESCE(shared_blk_read_time, 0) + COALESCE(shared_blk_write_time, 0) +
                 COALESCE(local_blk_read_time, 0) + COALESCE(local_blk_write_time, 0) +
                 COALESCE(temp_blk_read_time, 0) + COALESCE(temp_blk_write_time, 0)) as total_io_time,
                calls,
                query
            FROM pg_stat_statements
            ORDER BY total_io_time DESC LIMIT %(limit)s;
        """
    elif connector.has_pgstat_legacy_io_time:
        # PG13-16 style
        return """
            SELECT
                (blk_read_time + blk_write_time) as total_io_time,
                calls,
                query
            FROM pg_stat_statements
            ORDER BY total_io_time DESC LIMIT %(limit)s;
        """
    else:
        # Fallback for older versions or outdated extensions
        return """
            SELECT
                total_exec_time as total_exec_time_as_proxy_for_io,
                calls,
                query
            FROM pg_stat_statements
            ORDER BY total_time DESC LIMIT %(limit)s;
        """
