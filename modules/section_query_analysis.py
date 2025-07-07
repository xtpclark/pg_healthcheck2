def run_query_analysis(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Query Analysis", "Analyzes query performance metrics to identify bottlenecks and optimization opportunities."]
    
    if settings['show_qry'] == 'true':
        content.append("Query analysis queries:")
        content.append("[,sql]\n----")
        content.append("SELECT query, calls, total_exec_time, mean_exec_time, rows FROM pg_stat_statements WHERE calls > 0 ORDER BY total_exec_time DESC LIMIT %(limit)s;")
        content.append("SELECT state, count(*) AS query_count FROM pg_stat_activity WHERE datname = %(database)s AND query NOT LIKE 'autovacuum:%' GROUP BY state;")
        content.append("----")

    queries = [
        ("Top Queries by Execution Time", "SELECT query, calls, total_exec_time, mean_exec_time, rows FROM pg_stat_statements WHERE calls > 0 ORDER BY total_exec_time DESC LIMIT %(limit)s;", settings['has_pgstat'] == 't'),
        ("Active Query States", "SELECT state, count(*) AS query_count FROM pg_stat_activity WHERE datname = %(database)s AND query NOT LIKE 'autovacuum:%' GROUP BY state;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\n{'pg_stat_statements not installed.' if 'pg_stat_statements' in query else 'Query not applicable.'}\n====")
            continue
        params = {'database': settings['database'], 'limit': settings['row_limit']} if '%(' in query else None
        result = execute_query(query, params=params)
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\nHigh total_exec_time or mean_exec_time indicates resource-intensive queries; optimize them using indexes or query rewriting. Monitor active query counts to detect contention. For Aurora, use CloudWatch to track query performance and CPUUsage.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora integrates with CloudWatch for query performance monitoring. Enable pg_stat_statements for detailed query metrics.\n====")
    
    return "\n".join(content)
