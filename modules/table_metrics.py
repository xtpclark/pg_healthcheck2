def run_table_metrics(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Table Metrics", "Analyzes table sizes and live/dead tuples to identify potential issues."]
    if settings['show_qry'] == 'true':
        content.append("Table metrics queries:")
        content.append("[,sql]\n----")
        content.append("SELECT schemaname||'.'||relname AS table_name, pg_size_pretty(pg_total_relation_size(relid)) AS size FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT %(limit)s;")
        content.append("SELECT relname, n_live_tup AS live_tuples, n_dead_tup AS dead_tuples FROM pg_stat_user_tables WHERE n_dead_tup > 0 ORDER BY n_dead_tup DESC LIMIT %(limit)s;")
        content.append("SELECT relname, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze FROM pg_stat_user_tables WHERE last_autovacuum IS NOT NULL ORDER BY last_autovacuum DESC LIMIT %(limit)s;")
        content.append("----")

    queries = [
        ("Table Sizes", "SELECT schemaname||'.'||relname AS table_name, pg_size_pretty(pg_total_relation_size(relid)) AS size FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT %(limit)s;"),
        ("Live and Dead Tuples", "SELECT relname, n_live_tup AS live_tuples, n_dead_tup AS dead_tuples FROM pg_stat_user_tables WHERE n_dead_tup > 0 ORDER BY n_dead_tup DESC LIMIT %(limit)s;"),
        ("Vacuum and Analyze Status", "SELECT relname, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze FROM pg_stat_user_tables WHERE last_autovacuum IS NOT NULL ORDER BY last_autovacuum DESC LIMIT %(limit)s;")
    ]

    for title, query in queries:
        content.append(title)
        content.append(execute_query(query, params={'limit': settings['row_limit']}))
    
    content.append("[TIP]\n====\nHigh dead tuples may indicate autovacuum tuning needs. Run VACUUM ANALYZE on large tables.\n====")
    return "\n".join(content)
