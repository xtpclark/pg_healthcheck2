def get_high_insert_tables_query(connector):
    """
    Returns a query to identify tables with a high rate of inserts.
    This query is version-agnostic but centralized for consistency.
    """
    return """
        SELECT
            schemaname || '.' || relname AS table_name,
            n_tup_ins,
            n_dead_tup,
            last_autovacuum
        FROM pg_stat_user_tables
        WHERE n_tup_ins > %(min_tup_ins_threshold)s
        ORDER BY n_tup_ins DESC
        LIMIT %(limit)s;
    """

