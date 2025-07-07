def run_vacstat2(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Vacuum Progress and Statistics", "Analyzes ongoing vacuum operations and historical vacuum statistics to optimize performance and reduce bloat."]
    
    if settings['show_qry'] == 'true':
        content.append("Vacuum progress and statistics queries:")
        content.append("[,sql]\n----")
        content.append("SELECT n.nspname||'.'||c.relname AS table_name, v.phase, v.heap_blks_total, v.heap_blks_scanned, v.heap_blks_vacuumed, v.index_vacuum_count, v.num_dead_tuples FROM pg_stat_progress_vacuum v JOIN pg_class c ON v.relid = c.oid JOIN pg_namespace n ON c.relnamespace = n.oid WHERE v.datname = %(database)s;")
        content.append("SELECT schemaname||'.'||relname AS table_name, autovacuum_count, last_autovacuum, autoanalyze_count, last_autoanalyze FROM pg_stat_user_tables WHERE autovacuum_count > 0 ORDER BY autovacuum_count DESC LIMIT %(limit)s;")
        content.append("----")

    queries = [
        ("Ongoing Vacuum Operations", "SELECT n.nspname||'.'||c.relname AS table_name, v.phase, v.heap_blks_total, v.heap_blks_scanned, v.heap_blks_vacuumed, v.index_vacuum_count, v.num_dead_tuples FROM pg_stat_progress_vacuum v JOIN pg_class c ON v.relid = c.oid JOIN pg_namespace n ON c.relnamespace = n.oid WHERE v.datname = %(database)s;", True),
        ("Historical Vacuum Statistics", "SELECT schemaname||'.'||relname AS table_name, autovacuum_count, last_autovacuum, autoanalyze_count, last_autoanalyze FROM pg_stat_user_tables WHERE autovacuum_count > 0 ORDER BY autovacuum_count DESC LIMIT %(limit)s;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        result = execute_query(query, params={'database': settings['database'], 'limit': settings['row_limit']} if '%(' in query else {})
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\nMonitor ongoing vacuum operations to ensure they complete without excessive CPU or IOPS usage. High autovacuum_count may indicate frequent updates; tune autovacuum_vacuum_threshold or autovacuum_vacuum_cost_limit. For Aurora, adjust these settings via the RDS parameter group.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nFor AWS RDS Aurora, vacuum performance is influenced by parameter group settings. Use AWS Console to adjust autovacuum parameters.\n====")
    
    return "\n".join(content)
