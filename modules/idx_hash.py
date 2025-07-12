def run_hash_idx(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes Hash indexes in the PostgreSQL database.
    Hash indexes are used for equality comparisons and are memory-efficient for exact matches.
    """
    adoc_content = ["=== Hash Indexes\n", "Analyzes Hash indexes in the PostgreSQL database.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Import version compatibility module
    from .postgresql_version_compatibility import get_postgresql_version, validate_postgresql_version
    
    # Get PostgreSQL version compatibility information
    compatibility = get_postgresql_version(cursor, execute_query)
    
    # Validate PostgreSQL version
    is_supported, error_msg = validate_postgresql_version(compatibility)
    if not is_supported:
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["version_error"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data

    # Check if Hash indexes are supported (PostgreSQL 10+)
    if compatibility['version_num'] < 100000:
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append("Hash indexes require PostgreSQL 10 or newer. ")
        adoc_content.append(f"Current version: {compatibility['version_string']}\n")
        adoc_content.append("====\n")
        structured_data["hash_indexes"] = {"status": "not_supported", "reason": "PostgreSQL version too old"}
        return "\n".join(adoc_content), structured_data
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Hash index queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT n.nspname||'.'||c.relname AS table_name, i.relname AS index_name, am.amname AS index_type, pg_size_pretty(pg_relation_size(i.oid)) AS index_size FROM pg_index idx JOIN pg_class c ON idx.indrelid = c.oid JOIN pg_class i ON idx.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'hash' ORDER BY n.nspname, c.relname LIMIT %(limit)s;")
        adoc_content.append("SELECT n.nspname||'.'||c.relname AS table_name, s.indexrelname AS index_name, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch FROM pg_stat_user_indexes s JOIN pg_class c ON s.relid = c.oid JOIN pg_class i ON s.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'hash' ORDER BY s.idx_scan DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "Hash Index Details", 
            "SELECT n.nspname||'.'||c.relname AS table_name, i.relname AS index_name, am.amname AS index_type, pg_size_pretty(pg_relation_size(i.oid)) AS index_size FROM pg_index idx JOIN pg_class c ON idx.indrelid = c.oid JOIN pg_class i ON idx.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'hash' ORDER BY n.nspname, c.relname LIMIT %(limit)s;", 
            True,
            "hash_index_details"
        ),
        (
            "Hash Index Usage Statistics", 
            "SELECT n.nspname||'.'||c.relname AS table_name, s.indexrelname AS index_name, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch FROM pg_stat_user_indexes s JOIN pg_class c ON s.relid = c.oid JOIN pg_class i ON s.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'hash' ORDER BY s.idx_scan DESC LIMIT %(limit)s;", 
            True,
            "hash_index_usage_statistics"
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
    adoc_content.append("Hash indexes are optimized for equality comparisons (`=`) and are not suitable for range queries (`<`, `>`, `BETWEEN`). ")
    adoc_content.append("They are memory-efficient and provide fast lookups for exact matches. ")
    adoc_content.append("Hash indexes are particularly useful for columns with high cardinality where exact equality searches are common. ")
    adoc_content.append("Monitor `idx_scan` to ensure hash indexes are being utilized for relevant equality queries.\n")
    adoc_content.append("====\n")
    
    if settings.get('is_aurora', False):
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append("AWS RDS Aurora supports Hash indexes. ")
        adoc_content.append("Their performance will be influenced by the instance type and memory allocation. ")
        adoc_content.append("Monitor `CPUUtilization` and memory usage during heavy hash index operations.\n")
        adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data 