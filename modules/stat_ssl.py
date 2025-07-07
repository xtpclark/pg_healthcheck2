def run_stat_ssl(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes PostgreSQL SSL connection statistics to ensure secure communication
    and identify non-SSL connections.
    """
    adoc_content = ["=== SSL Connection Statistics", "Analyzes SSL connection usage to ensure secure communication with the database."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("SSL connection statistics queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT usename, client_addr, ssl, count(*) FROM pg_stat_ssl JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid GROUP BY usename, client_addr, ssl ORDER BY ssl DESC, count(*) DESC LIMIT %(limit)s;")
        adoc_content.append("SELECT ssl, count(*) AS connection_count FROM pg_stat_ssl GROUP BY ssl ORDER BY ssl DESC;")
        adoc_content.append("----")

    queries = [
        (
            "SSL Connections by User and Client Address", 
            "SELECT usename, client_addr, ssl, count(*) FROM pg_stat_ssl JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid GROUP BY usename, client_addr, ssl ORDER BY ssl DESC, count(*) DESC LIMIT %(limit)s;", 
            True,
            "ssl_connections_by_user_client" # Data key
        ),
        (
            "Overall SSL Usage Summary", 
            "SELECT ssl, count(*) AS connection_count FROM pg_stat_ssl GROUP BY ssl ORDER BY ssl DESC;", 
            True,
            "overall_ssl_usage_summary" # Data key
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
                   "Ensure that `ssl` is 'true' for all critical connections to protect data in transit. "
                   "If you see 'false' for `ssl` on connections that should be encrypted, investigate client configurations. "
                   "For Aurora, SSL is typically enforced at the instance level and managed via parameter groups and security groups.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora strongly encourages and often enforces SSL for client connections. "
                       "You can configure the `rds.force_ssl` parameter in the DB cluster parameter group to require SSL for all connections. "
                       "CloudWatch Logs can provide insights into connection attempts and their SSL status.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
