"""
Query library for the missing_index_opportunities check.
"""

def get_missing_index_opportunities_query():
    """
    Returns a query that finds large tables that are frequently read using
    inefficient sequential scans.
    """
    # The limit parameter is supplied by the calling check module.
    return """
        SELECT
            schemaname AS schema_name,
            relname AS table_name,
            seq_scan AS sequential_scans,
            pg_size_pretty(pg_relation_size(relid)) AS table_size,
            n_live_tup AS live_rows
        FROM pg_stat_user_tables
        WHERE seq_scan > 1000 AND n_live_tup > 10000 -- Scanned > 1000 times and > 10,000 rows
        ORDER BY seq_scan DESC
        LIMIT %(limit)s;
    """
