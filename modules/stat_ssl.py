def run_stat_ssl(cursor, settings, execute_query, execute_pgbouncer):
    """
    Analyzes PostgreSQL SSL connection statistics to ensure secure communication
    and identify non-SSL connections.
    """
    content = ["=== SSL Connection Statistics", "Analyzes SSL connection usage to ensure secure communication with the database."]
    
    if settings['show_qry'] == 'true':
        content.append("SSL connection statistics queries:")
        content.append("[,sql]\n----")
        content.append("SELECT usename, client_addr, ssl, count(*) FROM pg_stat_ssl JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid GROUP BY usename, client_addr, ssl ORDER BY ssl DESC, count(*) DESC LIMIT %(limit)s;")
        content.append("SELECT ssl, count(*) AS connection_count FROM pg_stat_ssl GROUP BY ssl ORDER BY ssl DESC;")
        content.append("----")

    queries = [
        (
            "SSL Connections by User and Client Address", 
            "SELECT usename, client_addr, ssl, count(*) FROM pg_stat_ssl JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid GROUP BY usename, client_addr, ssl ORDER BY ssl DESC, count(*) DESC LIMIT %(limit)s;", 
            True
        ),
        (
            "Overall SSL Usage Summary", 
            "SELECT ssl, count(*) AS connection_count FROM pg_stat_ssl GROUP BY ssl ORDER BY ssl DESC;", 
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
                   "Ensure that `ssl` is 'true' for all critical connections to protect data in transit. "
                   "If you see 'false' for `ssl` on connections that should be encrypted, investigate client configurations. "
                   "For Aurora, SSL is typically enforced at the instance level and managed via parameter groups and security groups.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora strongly encourages and often enforces SSL for client connections. "
                       "You can configure the `rds.force_ssl` parameter in the DB cluster parameter group to require SSL for all connections. "
                       "CloudWatch Logs can provide insights into connection attempts and their SSL status.\n"
                       "====\n")
    
    return "\n".join(content)

