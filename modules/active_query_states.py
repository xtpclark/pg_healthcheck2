def run_active_query_states(cursor, settings, execute_query, execute_pgbouncer):
    """
    Analyzes the states of active queries in pg_stat_activity to identify
    potential contention, idle sessions, or long-running operations.
    """
    content = ["=== Active Query States", "Analyzes the states of active queries to identify contention or idle sessions."]
    
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
        content.append("Active query states query:")
        content.append("[,sql]\n----")
        content.append(active_query_states_query)
        content.append("----")

    # Standardized parameter passing pattern:
    params_for_query = {
        'database': settings['database'],
        'autovacuum_pattern': 'autovacuum:%' # Pass the pattern as a named parameter
    }
    
    result = execute_query(active_query_states_query, params=params_for_query)
    
    if "[ERROR]" in result or "[NOTE]" in result:
        content.append(f"Active Query States\n{result}")
    else:
        content.append("Active Query States")
        content.append(result)
    
    content.append("[TIP]\n====\n"
                   "Monitoring active query states helps in understanding current database workload and identifying issues. "
                   "High counts of 'idle in transaction' indicate uncommitted transactions, which can hold locks and prevent vacuuming. "
                   "'waiting' states point to lock contention. "
                   "Regularly review `pg_stat_activity` to pinpoint problematic sessions and queries.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora provides `DatabaseConnections` and `ActiveConnections` metrics in CloudWatch. "
                       "Detailed query states are visible via `pg_stat_activity` within the database. "
                       "Address high counts of 'idle in transaction' or 'waiting' to improve performance and resource utilization.\n"
                       "====\n")
    
    return "\n".join(content)

