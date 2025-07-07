def run_current_lock_waits(cursor, settings, execute_query, execute_pgbouncer):
    """
    Identifies current sessions that are waiting for locks, indicating
    potential contention or deadlock situations.
    """
    content = ["=== Current Lock Waits", "Identifies current sessions waiting for locks."]
    
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
        content.append("Current lock waits query:")
        content.append("[,sql]\n----")
        content.append(current_lock_waits_query)
        content.append("----")

    # Standardized parameter passing pattern:
    params_for_query = {
        'database': settings['database'],
        'limit': settings['row_limit'],
        'autovacuum_pattern': 'autovacuum:%' # Pass the pattern as a named parameter
    }

    result = execute_query(current_lock_waits_query, params=params_for_query)
    
    if "[ERROR]" in result or "[NOTE]" in result:
        content.append(f"Current Lock Waits\n{result}")
    else:
        content.append("Current Lock Waits")
        content.append(result)
    
    content.append("[TIP]\n====\n"
                   "Sessions in a 'waiting' state for a lock indicate active contention. "
                   "Investigate the `query` of the waiting session and the session holding the lock (which will have `pl.granted = true` for the same lock). "
                   "Optimize conflicting queries with better indexing, reduce transaction isolation levels if appropriate, or implement proper transaction management. "
                   "Persistent lock waits can degrade performance and lead to deadlocks.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora provides enhanced monitoring metrics for lock contention. "
                       "Use CloudWatch to set alarms for high `LockWaits` or `Deadlocks` metrics. "
                       "Query optimization remains the primary method to reduce lock contention in Aurora.\n"
                       "====\n")
    
    return "\n".join(content)

