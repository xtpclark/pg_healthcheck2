def run_connection_metrics(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes PostgreSQL connection metrics to monitor database load and identify
    potential connection-related issues.
    """
    adoc_content = ["=== Connection Metrics", "Analyzes database connection statistics to monitor load and identify potential bottlenecks."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Connection metrics queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT count(*) AS total_connections, (SELECT setting FROM pg_settings WHERE name = 'max_connections')::int AS max_connections FROM pg_stat_activity;")
        adoc_content.append("SELECT state, count(*) FROM pg_stat_activity GROUP BY state ORDER BY count(*) DESC;")
        adoc_content.append("SELECT usename, datname, count(*) FROM pg_stat_activity GROUP BY usename, datname ORDER BY count(*) DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "Total Connections and Limits", 
            "SELECT count(*) AS total_connections, (SELECT setting FROM pg_settings WHERE name = 'max_connections')::int AS max_connections FROM pg_stat_activity;", 
            True,
            "total_connections_and_limits" # Data key
        ),
        (
            "Connection States", 
            "SELECT state, count(*) FROM pg_stat_activity GROUP BY state ORDER BY count(*) DESC;", 
            True,
            "connection_states" # Data key
        ),
        (
            "Connections by User and Database", 
            "SELECT usename, datname, count(*) FROM pg_stat_activity GROUP BY usename, datname ORDER BY count(*) DESC LIMIT %(limit)s;", 
            True,
            "connections_by_user_database" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\n"
                   "Monitor the total number of connections relative to `max_connections` to prevent connection exhaustion. "
                   "High numbers of 'idle in transaction' or 'waiting' states may indicate application issues or lock contention. "
                   "Optimize queries or adjust connection pooling settings to reduce idle connections. "
                   "For Aurora, `max_connections` is managed via the parameter group, and connection monitoring can be done via CloudWatch.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora automatically scales connections to some extent, but `max_connections` still defines the hard limit. "
                       "Use CloudWatch metrics like `DatabaseConnections` to track usage. "
                       "Consider using Amazon RDS Proxy for efficient connection management, especially for serverless applications.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

