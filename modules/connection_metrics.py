def run_connection_metrics(cursor, settings, execute_query, execute_pgbouncer):
    """
    Analyzes PostgreSQL connection metrics to monitor database load and identify
    potential connection-related issues.
    """
    content = ["=== Connection Metrics", "Analyzes database connection statistics to monitor load and identify potential bottlenecks."]
    
    if settings['show_qry'] == 'true':
        content.append("Connection metrics queries:")
        content.append("[,sql]\n----")
        content.append("SELECT count(*) AS total_connections, (SELECT setting FROM pg_settings WHERE name = 'max_connections')::int AS max_connections FROM pg_stat_activity;")
        content.append("SELECT state, count(*) FROM pg_stat_activity GROUP BY state ORDER BY count(*) DESC;")
        content.append("SELECT usename, datname, count(*) FROM pg_stat_activity GROUP BY usename, datname ORDER BY count(*) DESC LIMIT %(limit)s;")
        content.append("----")

    queries = [
        (
            "Total Connections and Limits", 
            "SELECT count(*) AS total_connections, (SELECT setting FROM pg_settings WHERE name = 'max_connections')::int AS max_connections FROM pg_stat_activity;", 
            True
        ),
        (
            "Connection States", 
            "SELECT state, count(*) FROM pg_stat_activity GROUP BY state ORDER BY count(*) DESC;", 
            True
        ),
        (
            "Connections by User and Database", 
            "SELECT usename, datname, count(*) FROM pg_stat_activity GROUP BY usename, datname ORDER BY count(*) DESC LIMIT %(limit)s;", 
            True
        )
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        
        # Standardized parameter passing pattern:
        # Check if the query contains the %(limit)s placeholder and pass params accordingly.
        params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        
        result = execute_query(query, params=params_for_query)
        
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\n"
                   "Monitor the total number of connections relative to `max_connections` to prevent connection exhaustion. "
                   "High numbers of 'idle in transaction' or 'waiting' states may indicate application issues or lock contention. "
                   "Optimize queries or adjust connection pooling settings to reduce idle connections. "
                   "For Aurora, `max_connections` is managed via the parameter group, and connection monitoring can be done via CloudWatch.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora automatically scales connections to some extent, but `max_connections` still defines the hard limit. "
                       "Use CloudWatch metrics like `DatabaseConnections` to track usage. "
                       "Consider using Amazon RDS Proxy for efficient connection management, especially for serverless applications.\n"
                       "====\n")
    
    return "\n".join(content)

