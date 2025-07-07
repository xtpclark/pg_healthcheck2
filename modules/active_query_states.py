def run_active_query_states(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes the states of active queries in pg_stat_activity to identify
    potential contention, idle sessions, or long-running operations.
    """
    adoc_content = ["=== Active Query States", "Analyzes the states of active queries to identify contention or idle sessions."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Define the query string with named parameters for robustness
    active_query_states_query = """
        SELECT state, count(*) AS query_count
        FROM pg_stat_activity
        WHERE datname = %(database)s
          AND query NOT LIKE %(autovacuum_pattern)s
        GROUP BY state
        ORDER BY count(*) DESC;
    """

    if settings['show_qry'] == 'true':
        adoc_content.append("Active query states query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(active_query_states_query)
        adoc_content.append("----")

    # Standardized parameter passing pattern:
    params_for_query = {
        'database': settings['database'],
        'autovacuum_pattern': 'autovacuum:%' # Pass the pattern as a named parameter
    }
    
    formatted_result, raw_result = execute_query(active_query_states_query, params=params_for_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Active Query States\n{formatted_result}")
        structured_data["active_query_states"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("Active Query States")
        adoc_content.append(formatted_result)
        structured_data["active_query_states"] = {"status": "success", "data": raw_result}
    
    adoc_content.append("[TIP]\n====\n"
                   "Monitoring active query states helps in understanding current database workload and identifying issues. "
                   "High counts of 'idle in transaction' indicate uncommitted transactions, which can hold locks and prevent vacuuming. "
                   "'waiting' states point to lock contention. "
                   "Regularly review `pg_stat_activity` to pinpoint problematic sessions and queries.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora provides `DatabaseConnections` and `ActiveConnections` metrics in CloudWatch. "
                       "Detailed query states are visible via `pg_stat_activity` within the database. "
                       "Address high counts of 'idle in transaction' or 'waiting' to improve performance and resource utilization.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
