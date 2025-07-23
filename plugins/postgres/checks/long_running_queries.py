def run_long_running_queries(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies long-running queries that may contribute to performance issues.
    """
    adoc_content = ["=== Long-Running Queries", "Identifies long-running queries that may contribute to performance issues.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
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
        adoc_content.append("Long-running queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(long_running_query_string)
        adoc_content.append("----")

    queries = [
        (
            "Long-Running Queries", 
            long_running_query_string, 
            True,
            "long_running_queries_list" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        # Construct params for this specific query, including the new autovacuum_pattern
        params_for_query = {
            'database': settings['database'],
            'limit': settings['row_limit'],
            'autovacuum_pattern': 'autovacuum:%' # Pass the pattern as a named parameter
        }
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nLong-running queries (duration > 1 minute) may cause CPU or I/O bottlenecks. Investigate and optimize these queries, or terminate them if necessary (e.g., using pg_terminate_backend(pid)). For Aurora, monitor long-running queries via CloudWatch and consider query optimization or scaling.\n====")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora can experience CPU saturation from long-running queries. Use CloudWatch to set alerts for high CPUUsage or QueryLatency.\n====")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

