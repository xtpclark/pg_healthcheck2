def run_deadlocks(cursor, settings, execute_query, execute_pgbouncer):
#    content = ["=== Deadlock Analysis", "Analyzes deadlock occurrences to identify contention issues impacting performance."]
    content = ["Analyzes deadlock occurrences to identify contention issues impacting performance.\n"]
    
    # Define the base query strings with named parameters
    lock_wait_config_query = "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name = 'log_lock_waits';"
    
    # Modified query to join with pg_locks for lock details
    current_lock_waits_query = """
        SELECT
            psa.pid,
            psa.usename,
            pl.locktype, -- From pg_locks
            pl.mode,     -- From pg_locks
            pl.granted,  -- From pg_locks
            psa.query
        FROM
            pg_stat_activity psa
        JOIN
            pg_locks pl ON psa.pid = pl.pid
        WHERE
            psa.datname = %(database)s
            AND psa.wait_event_type = 'Lock' -- Filter activity by wait event type
            AND pl.granted = false -- Only show locks that are not granted (i.e., sessions waiting)
            AND psa.query NOT LIKE %(autovacuum_pattern)s
        LIMIT %(limit)s;
    """

    if settings['show_qry'] == 'true':
        content.append("Deadlock queries:")
        content.append("[,sql]\n----")
        content.append(lock_wait_config_query)
        content.append(current_lock_waits_query)
        content.append("----")

    queries = [
        ("Lock Wait Configuration", lock_wait_config_query, True),
        ("Current Lock Waits", current_lock_waits_query, True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = None
        if '%(database)s' in query or '%(limit)s' in query or '%(autovacuum_pattern)s' in query:
            params_for_query = {
                'database': settings['database'],
                'limit': settings['row_limit'],
                'autovacuum_pattern': 'autovacuum:%' # Pass the pattern as a named parameter
            }

        result = execute_query(query, params=params_for_query)
        
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\nEnable log_lock_waits to detect deadlocks in logs. Investigate lock waits to identify conflicting queries and optimize them with better indexing or transaction management. For Aurora, monitor lock contention via CloudWatch and consider query optimization to reduce CPU saturation.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora logs lock waits to CloudWatch Logs if log_lock_waits is enabled. Use CloudWatch to set alerts for lock contention.\n====")
    
    return "\n".join(content)

