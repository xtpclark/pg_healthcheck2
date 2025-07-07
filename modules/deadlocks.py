def run_deadlocks(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Deadlock Analysis", "Analyzes deadlock occurrences to identify contention issues impacting performance."]
    
    if settings['show_qry'] == 'true':
        content.append("Deadlock queries:")
        content.append("[,sql]\n----")
        content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name = 'log_lock_waits';")
        content.append("SELECT pid, usename, locktype, mode, granted, query FROM pg_stat_activity WHERE datname = %(database)s AND wait_event_type = 'Lock' AND query NOT LIKE 'autovacuum:%' LIMIT %(limit)s;")
        content.append("----")

    queries = [
        ("Lock Wait Configuration", "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name = 'log_lock_waits';", True),
        ("Current Lock Waits", "SELECT pid, usename, locktype, mode, granted, query FROM pg_stat_activity WHERE datname = %(database)s AND wait_event_type = 'Lock' AND query NOT LIKE 'autovacuum:%' LIMIT %(limit)s;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        params = {'database': settings['database'], 'limit': settings['row_limit']} if '%(' in query else None
        result = execute_query(query, params=params)
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\nEnable log_lock_waits to detect deadlocks in logs. Investigate lock waits to identify conflicting queries and optimize them with better indexing or transaction management. For Aurora, monitor lock contention via CloudWatch and consider query optimization to reduce CPU saturation.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora logs lock waits to CloudWatch Logs if log_lock_waits is enabled. Use CloudWatch to set alerts for lock contention.\n====")
    
    return "\n".join(content)
