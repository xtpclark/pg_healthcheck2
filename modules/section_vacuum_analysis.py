def run_vacuum_analysis(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Vacuum and Bloat Analysis", "Analyzes vacuum activity, dead tuples, and transaction ID wraparound risks to optimize database performance and prevent bloat."]
    
    if settings['show_qry'] == 'true':
        content.append("Vacuum and bloat queries:")
        content.append("[,sql]\n----")
        content.append("SELECT schemaname||'.'||relname AS table_name, n_dead_tup AS dead_tuples, n_live_tup AS live_tuples, round((n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0)::numeric * 100)::numeric, 2) AS dead_tuple_percent, last_vacuum, last_autovacuum FROM pg_stat_user_tables WHERE n_dead_tup > 0 ORDER BY n_dead_tup DESC LIMIT %(limit)s;")
        content.append("SELECT count(*) AS vacuum_processes FROM pg_stat_activity WHERE query LIKE 'autovacuum:%';")
        content.append("SELECT greatest(max(age(datfrozenxid)), max(age(datminmxid))) AS max_xid_age FROM pg_database WHERE datname = %(database)s;")
        content.append("----")

    queries = [
        ("Tables with High Dead Tuples", "SELECT schemaname||'.'||relname AS table_name, n_dead_tup AS dead_tuples, n_live_tup AS live_tuples, round((n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0)::numeric * 100)::numeric, 2) AS dead_tuple_percent, last_vacuum, last_autovacuum FROM pg_stat_user_tables WHERE n_dead_tup > 0 ORDER BY n_dead_tup DESC LIMIT %(limit)s;", True),
        ("Active Vacuum Processes", "SELECT count(*) AS vacuum_processes FROM pg_stat_activity WHERE query LIKE 'autovacuum:%';", True),
        ("Transaction ID Age", "SELECT greatest(max(age(datfrozenxid)), max(age(datminmxid))) AS max_xid_age FROM pg_database WHERE datname = %(database)s;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        content.append(title)
        content.append(execute_query(query, params={'database': settings['database'], 'limit': settings['row_limit']}))
    
    content.append("[TIP]\n====\nHigh dead tuple percentages (>10%) indicate potential bloat; consider tuning autovacuum settings (e.g., autovacuum_vacuum_cost_limit) or running manual VACUUM FULL. Monitor max_xid_age to prevent transaction ID wraparound; values above 1 billion require immediate VACUUM FREEZE. For Aurora, adjust autovacuum parameters via the RDS parameter group.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nFor AWS RDS Aurora, autovacuum settings are managed via the parameter group. Use AWS Console to adjust parameters like autovacuum_vacuum_cost_limit.\n====")
    
    return "\n".join(content)
