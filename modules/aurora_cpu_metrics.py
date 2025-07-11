def run_aurora_cpu_metrics(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes CPU and IOPS usage to identify saturation issues on the writer node.
    """
    adoc_content = ["Analyzes CPU and IOPS usage to identify saturation issues on the writer node.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module

    # Define the autovacuum pattern for LIKE clauses
    autovacuum_pattern = 'autovacuum:%'
    
    if settings['show_qry'] == 'true':
        adoc_content.append("CPU and IOPS queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT replica_lag, replica_lag_size FROM aurora_replica_status();")
        adoc_content.append(f"SELECT state, count(*) FROM pg_stat_activity WHERE state = 'active' AND query NOT LIKE '{autovacuum_pattern}' GROUP BY state;")
        adoc_content.append("SELECT query, calls, total_exec_time, temp_blks_written FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "Aurora Replication Metrics", 
            "SELECT replica_lag, replica_lag_size FROM aurora_replica_status();", 
            settings['is_aurora'] == 'true', # Condition: Only for Aurora
            "aurora_replication_metrics" # Data key
        ),
        (
            "Active Connections (Excluding Autovacuum)", 
            f"SELECT state, count(*) FROM pg_stat_activity WHERE state = 'active' AND query NOT LIKE %(autovacuum_pattern)s GROUP BY state;", 
            True, # Always run if pg_stat_activity is available
            "active_connections" # Data key
        ),
        (
            "Top CPU-Intensive Queries (from pg_stat_statements)", 
            "SELECT query, calls, total_exec_time, temp_blks_written FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT %(limit)s;", 
            settings['has_pgstat'] == 't', # Condition: Only if pg_stat_statements is installed
            "top_cpu_intensive_queries" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            note_msg = 'Aurora-specific metrics not available.' if 'aurora_replica_status' in query else 'pg_stat_statements not installed.'
            adoc_content.append(f"{title}\n[NOTE]\n====\n{note_msg}\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": note_msg}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {}
        if '%(limit)s' in query:
            params_for_query['limit'] = settings['row_limit']
        if '%(autovacuum_pattern)s' in query:
            params_for_query['autovacuum_pattern'] = autovacuum_pattern

        params_to_pass = params_for_query if params_for_query else None

        formatted_result, raw_result = execute_query(query, params=params_to_pass, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nOptimize high-CPU queries or scale up the writer node. Monitor CPUUsage in AWS CloudWatch.\n====")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora can experience CPU saturation from long-running queries. Use CloudWatch to set alerts for high CPUUsage or QueryLatency.\n====")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

