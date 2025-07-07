def run_top_queries_by_execution_time(cursor, settings, execute_query, execute_pgbouncer):
    """
    Analyzes top queries by total execution time from pg_stat_statements
    to identify resource-intensive queries.
    """
    content = ["=== Top Queries by Execution Time", "Identifies resource-intensive queries based on total execution time."]
    
    # Define the query string
    top_queries_query = """
        SELECT query, calls, total_exec_time, mean_exec_time, rows
        FROM pg_stat_statements
        WHERE calls > 0
        ORDER BY total_exec_time DESC LIMIT %(limit)s;
    """

    if settings['show_qry'] == 'true':
        content.append("Top queries by execution time query:")
        content.append("[,sql]\n----")
        content.append(top_queries_query)
        content.append("----")

    # Check condition for pg_stat_statements
    condition = settings['has_pgstat'] == 't'

    if not condition:
        content.append("[NOTE]\n====\npg_stat_statements extension is not installed or enabled. Install pg_stat_statements to analyze top queries.\n====\n")
    else:
        # Standardized parameter passing pattern:
        params_for_query = {'limit': settings['row_limit']}
        result = execute_query(top_queries_query, params=params_for_query)
        
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"Top Queries by Execution Time\n{result}")
        else:
            content.append("Top Queries by Execution Time")
            content.append(result)
    
    content.append("[TIP]\n====\n"
                   "Queries with high `total_exec_time` or `mean_exec_time` are consuming significant database resources. "
                   "Investigate these queries for optimization opportunities, such as adding appropriate indexes, rewriting inefficient parts, or adjusting application logic. "
                   "For Aurora, optimizing these queries directly reduces `CPUUtilization` and improves overall performance.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora integrates `pg_stat_statements` for detailed query performance monitoring. "
                       "Use CloudWatch to correlate high `CPUUtilization` or `DatabaseConnections` with specific long-running or frequently executed queries identified here.\n"
                       "====\n")
    
    return "\n".join(content)

