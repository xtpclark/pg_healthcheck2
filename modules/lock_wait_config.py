def run_lock_wait_config(cursor, settings, execute_query, execute_pgbouncer):
    """
    Analyzes the 'log_lock_waits' configuration setting, which is crucial
    for detecting and diagnosing deadlocks and lock contention.
    """
    content = ["=== Lock Wait Configuration", "Analyzes the 'log_lock_waits' setting for deadlock detection."]
    
    if settings['show_qry'] == 'true':
        content.append("Lock wait configuration query:")
        content.append("[,sql]\n----")
        content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name = 'log_lock_waits';")
        content.append("----")

    query = "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name = 'log_lock_waits';"
    
    # No parameters needed for this query
    params_for_query = None 
    result = execute_query(query, params=params_for_query)
    
    if "[ERROR]" in result or "[NOTE]" in result:
        content.append(f"Lock Wait Configuration\n{result}")
    else:
        content.append("Lock Wait Configuration")
        content.append(result)
    
    content.append("[TIP]\n====\n"
                   "The `log_lock_waits` parameter (when set to `on`) is essential for PostgreSQL to log information about sessions waiting for locks. "
                   "This helps in identifying queries that are frequently involved in lock contention or deadlocks. "
                   "If you are experiencing performance issues related to concurrency, ensure this setting is enabled to aid in diagnosis.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, `log_lock_waits` can be enabled via the DB cluster parameter group. "
                       "Logs related to lock waits will be sent to CloudWatch Logs, allowing for centralized monitoring and alerting on lock contention.\n"
                       "====\n")
    
    return "\n".join(content)

