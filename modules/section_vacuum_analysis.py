def run_vacuum_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes vacuum activity, dead tuples, and transaction ID wraparound risks
    to optimize database performance and prevent bloat.
    """
    adoc_content = ["=== Vacuum and Bloat Analysis", "Analyzes vacuum activity, dead tuples, and transaction ID wraparound risks to optimize database performance and prevent bloat."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Define the autovacuum pattern for LIKE clauses
    autovacuum_pattern = 'autovacuum:%'

    if settings['show_qry'] == 'true':
        adoc_content.append("Vacuum and bloat queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT schemaname||'.'||relname AS table_name, n_dead_tup AS dead_tuples, n_live_tup AS live_tuples, round((n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0)::numeric * 100)::numeric, 2) AS dead_tuple_percent, last_vacuum, last_autovacuum FROM pg_stat_user_tables WHERE n_dead_tup > 0 ORDER BY n_dead_tup DESC LIMIT %(limit)s;")
        adoc_content.append(f"SELECT count(*) AS vacuum_processes FROM pg_stat_activity WHERE query LIKE '{autovacuum_pattern}';") # Use f-string for display query
        adoc_content.append("SELECT greatest(max(age(datfrozenxid)), max(age(datminmxid))) AS max_xid_age FROM pg_database WHERE datname = %(database)s;")
        adoc_content.append("----")

    queries = [
        (
            "Tables with High Dead Tuples", 
            "SELECT schemaname||'.'||relname AS table_name, n_dead_tup AS dead_tuples, n_live_tup AS live_tuples, round((n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0)::numeric * 100)::numeric, 2) AS dead_tuple_percent, last_vacuum, last_autovacuum FROM pg_stat_user_tables WHERE n_dead_tup > 0 ORDER BY n_dead_tup DESC LIMIT %(limit)s;", 
            True, 
            "high_dead_tuples" # Data key
        ),
        (
            "Active Vacuum Processes", 
            "SELECT count(*) AS vacuum_processes FROM pg_stat_activity WHERE query LIKE %(autovacuum_pattern)s;", # Use named parameter
            True, 
            "active_vacuum_processes" # Data key
        ),
        (
            "Transaction ID Age", 
            "SELECT greatest(max(age(datfrozenxid)), max(age(datminmxid))) AS max_xid_age FROM pg_database WHERE datname = %(database)s;", 
            True, 
            "transaction_id_age" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {}
        if '%(limit)s' in query:
            params_for_query['limit'] = settings['row_limit']
        if '%(database)s' in query:
            params_for_query['database'] = settings['database']
        if '%(autovacuum_pattern)s' in query:
            params_for_query['autovacuum_pattern'] = autovacuum_pattern # Pass the pattern as a named parameter

        # If params_for_query is empty, pass None to execute_query
        params_to_pass = params_for_query if params_for_query else None

        formatted_result, raw_result = execute_query(query, params=params_to_pass, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nHigh dead tuple percentages (>10%) indicate potential bloat; consider tuning autovacuum settings (e.g., autovacuum_vacuum_cost_limit) or running manual VACUUM FULL. Monitor max_xid_age to prevent transaction ID wraparound; values above 1 billion require immediate VACUUM FREEZE. For Aurora, adjust autovacuum parameters via the RDS parameter group.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nFor AWS RDS Aurora, autovacuum settings are managed via the parameter group. Use AWS Console to adjust parameters like autovacuum_vacuum_cost_limit.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

