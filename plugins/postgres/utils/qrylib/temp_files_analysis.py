"""
Query library for the temp_files_analysis check.
"""

def get_temp_files_query():
    """
    Returns a query that identifies queries from pg_stat_statements that are
    writing to or reading from temporary files on disk.
    """
    # The limit parameter is supplied by the calling check module.
    return """
        SELECT
            REPLACE(REPLACE(LEFT(pss.query, 100), E'\\n', ' '), '|', ' ') || '...' AS query,
            pss.calls,
            pg_size_pretty(pss.temp_blks_written * 8192) AS total_temp_written,
            pg_size_pretty(pss.temp_blks_read * 8192) AS total_temp_read
        FROM pg_stat_statements pss
        WHERE (pss.temp_blks_written > 0 OR pss.temp_blks_read > 0)
        ORDER BY (pss.temp_blks_written + pss.temp_blks_read) DESC
        LIMIT %(limit)s;
    """
