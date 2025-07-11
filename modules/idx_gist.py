def run_gist_idx(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes GiST indexes in the PostgreSQL database.
    GiST indexes are used for geometric data types and full-text search with various data types.
    """
    adoc_content = ["=== GiST Indexes\n", "Analyzes GiST indexes in the PostgreSQL database.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("GiST index queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT n.nspname||'.'||c.relname AS table_name, i.relname AS index_name, am.amname AS index_type, pg_size_pretty(pg_relation_size(i.oid)) AS index_size FROM pg_index idx JOIN pg_class c ON idx.indrelid = c.oid JOIN pg_class i ON idx.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'gist' ORDER BY n.nspname, c.relname LIMIT %(limit)s;")
        adoc_content.append("SELECT n.nspname||'.'||c.relname AS table_name, s.indexrelname AS index_name, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch FROM pg_stat_user_indexes s JOIN pg_class c ON s.relid = c.oid JOIN pg_class i ON s.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'gist' ORDER BY s.idx_scan DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "GiST Index Details", 
            "SELECT n.nspname||'.'||c.relname AS table_name, i.relname AS index_name, am.amname AS index_type, pg_size_pretty(pg_relation_size(i.oid)) AS index_size FROM pg_index idx JOIN pg_class c ON idx.indrelid = c.oid JOIN pg_class i ON idx.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'gist' ORDER BY n.nspname, c.relname LIMIT %(limit)s;", 
            True,
            "gist_index_details"
        ),
        (
            "GiST Index Usage Statistics", 
            "SELECT n.nspname||'.'||c.relname AS table_name, s.indexrelname AS index_name, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch FROM pg_stat_user_indexes s JOIN pg_class c ON s.relid = c.oid JOIN pg_class i ON s.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'gist' ORDER BY s.idx_scan DESC LIMIT %(limit)s;", 
            True,
            "gist_index_usage_statistics"
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
    
    adoc_content.append("[TIP]\n====\n")
    adoc_content.append("GiST indexes are versatile and support various data types including geometric data, full-text search, and custom data types. ")
    adoc_content.append("They are commonly used with geometric data types (`point`, `box`, `circle`, `polygon`) and text search. ")
    adoc_content.append("GiST indexes provide efficient spatial operations and text search capabilities. ")
    adoc_content.append("Monitor `idx_scan` to ensure GiST indexes are being utilized for relevant spatial and text search queries.\n")
    adoc_content.append("====\n")
    
    if settings.get('is_aurora', False):
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append("AWS RDS Aurora supports GiST indexes for geometric data and full-text search. ")
        adoc_content.append("Their performance will be influenced by the instance type and storage I/O. ")
        adoc_content.append("Monitor `CPUUtilization` and `IOPS` during heavy geometric or text search operations.\n")
        adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data 