def run_long_running_queries(cursor, settings, execute_query, execute_pgbouncer):
    """
    Identifies long-running queries that may contribute to performance issues.
    """
    content = ["=== Long-Running Queries", "Identifies long-running queries that may contribute to performance issues."]
    
    # Define the base query string with named parameters, including the autovacuum pattern
    long_running_query_string = """
        SELECT pid, usename, query, now() - query_start AS duration
        FROM pg_stat_activity
        WHERE datname = %(database)s
          AND state = 'active'
          AND query NOT LIKE %(autovacuum_pattern)s -- Changed to use a named parameter
          AND now() - query_start > interval '1 minute'
        ORDER BY duration DESC LIMIT %(limit)s;
    """

    if settings['show_qry'] == 'true':
        content.append("Long-running queries:")
        content.append("[,sql]\n----")
        content.append(long_running_query_string)
        content.append("----")

    queries = [
        ("Long-Running Queries", long_running_query_string, True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        
        # Standardized parameter passing pattern:
        # Construct params for this specific query, including the new autovacuum_pattern
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
    
    content.append("[TIP]\n====\nLong-running queries (duration > 1 minute) may cause CPU or I/O bottlenecks. Investigate and optimize these queries, or terminate them if necessary (e.g., using pg_terminate_backend(pid)). For Aurora, monitor long-running queries via CloudWatch and consider query optimization or scaling.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora can experience CPU saturation from long-running queries. Use CloudWatch to set alerts for high CPUUsage or QueryLatency.\n====")
    
    return "\n".join(content)
