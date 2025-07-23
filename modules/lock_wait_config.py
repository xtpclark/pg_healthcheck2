def run_lock_wait_config(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes the 'log_lock_waits' configuration setting, which is crucial
    for detecting and diagnosing deadlocks and lock contention.
    """
    adoc_content = ["=== Lock Wait Configuration", "Analyzes the 'log_lock_waits' setting for deadlock detection.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Define the query string
    query = "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name = 'log_lock_waits';"

    if settings['show_qry'] == 'true':
        adoc_content.append("Lock wait configuration query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(query)
        adoc_content.append("----")

    # No parameters needed for this query
    params_for_query = None 
    
    formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Lock Wait Configuration\n{formatted_result}")
        structured_data["lock_wait_configuration"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("Lock Wait Configuration")
        adoc_content.append(formatted_result)
        structured_data["lock_wait_configuration"] = {"status": "success", "data": raw_result}
    
    adoc_content.append("[TIP]\n====\n"
                   "The `log_lock_waits` parameter (when set to `on`) is essential for PostgreSQL to log information about sessions waiting for locks. "
                   "This helps in identifying queries that are frequently involved in lock contention or deadlocks. "
                   "If you are experiencing performance issues related to concurrency, ensure this setting is enabled to aid in diagnosis.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, `log_lock_waits` can be enabled via the DB cluster parameter group. "
                       "Logs related to lock waits will be sent to CloudWatch Logs, allowing for centralized monitoring and alerting on lock contention.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
