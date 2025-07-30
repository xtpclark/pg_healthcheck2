def get_object_counts_query(connector):
    """
    Returns a single, efficient query to count various database object types.
    This serves as both the detailed data and the AI summary.
    """
    return """
        SELECT
            (SELECT COUNT(*) FROM pg_class WHERE relkind = 'r' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS tables,
            (SELECT COUNT(*) FROM pg_class WHERE relkind = 'i' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS indexes,
            (SELECT COUNT(*) FROM pg_class WHERE relkind = 'S' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS sequences,
            (SELECT COUNT(*) FROM pg_class WHERE relkind = 'v' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS views,
            (SELECT COUNT(*) FROM pg_class WHERE relkind = 'm' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS materialized_views,
            (SELECT COUNT(*) FROM pg_proc WHERE pronamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%%' OR nspname = 'information_schema')) AS functions_procedures,
            (SELECT COUNT(*) FROM pg_namespace WHERE nspname NOT LIKE 'pg_%%' AND nspname != 'information_schema') AS schemas,
            (SELECT COUNT(*) FROM pg_constraint WHERE contype = 'f') AS foreign_keys,
            (SELECT COUNT(*) FROM pg_class WHERE relispartition = true) AS partitions;
    """
