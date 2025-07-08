def run_gin_idx(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes GIN (Generalized Inverted Index) indexes to evaluate their usage and efficiency.
    GIN indexes are typically used for full-text search and array columns.
    """
    adoc_content = ["=== GIN Index Analysis", "Analyzes GIN (Generalized Inverted Index) indexes to evaluate their usage and efficiency.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("GIN index queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT n.nspname||'.'||c.relname AS table_name, i.relname AS index_name, am.amname AS index_type, pg_size_pretty(pg_relation_size(i.oid)) AS index_size FROM pg_index idx JOIN pg_class c ON idx.indrelid = c.oid JOIN pg_class i ON idx.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'gin' ORDER BY n.nspname, c.relname LIMIT %(limit)s;")
        adoc_content.append("SELECT n.nspname||'.'||c.relname AS table_name, s.indexrelname AS index_name, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch FROM pg_stat_user_indexes s JOIN pg_class c ON s.relid = c.oid JOIN pg_class i ON s.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'gin' ORDER BY s.idx_scan DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "GIN Index Details", 
            "SELECT n.nspname||'.'||c.relname AS table_name, i.relname AS index_name, am.amname AS index_type, pg_size_pretty(pg_relation_size(i.oid)) AS index_size FROM pg_index idx JOIN pg_class c ON idx.indrelid = c.oid JOIN pg_class i ON idx.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'gin' ORDER BY n.nspname, c.relname LIMIT %(limit)s;", 
            True,
            "gin_index_details" # Data key
        ),
        (
            "GIN Index Usage Statistics", 
            "SELECT n.nspname||'.'||c.relname AS table_name, s.indexrelname AS index_name, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch FROM pg_stat_user_indexes s JOIN pg_class c ON s.relid = c.oid JOIN pg_class i ON s.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'gin' ORDER BY s.idx_scan DESC LIMIT %(limit)s;", 
            True,
            "gin_index_usage_statistics" # Data key
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
                   "GIN indexes are best suited for columns that contain composite values, such as arrays or full-text search documents. "
                   "Monitor `idx_scan` to ensure GIN indexes are being utilized for relevant queries. "
                   "GIN indexes can be larger and slower to build/update than B-tree indexes, so ensure their benefits outweigh their overhead for your workload.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora supports GIN indexes. Their performance will be influenced by the instance type and storage I/O. "
                       "Ensure your application queries are optimized to leverage GIN indexes effectively, especially for full-text search operations. "
                       "Monitor `CPUUtilization` and `IOPS` during heavy GIN index usage.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

