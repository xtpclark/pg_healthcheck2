def run_brin_idx(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes BRIN (Block Range Index) indexes to evaluate their usage and efficiency.
    """
    adoc_content = ["=== BRIN Index Analysis", "Analyzes BRIN (Block Range Index) indexes to evaluate their usage and efficiency."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("BRIN index queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT n.nspname||'.'||c.relname AS table_name, i.relname AS index_name, am.amname AS index_type, pg_size_pretty(pg_relation_size(i.oid)) AS index_size FROM pg_index idx JOIN pg_class c ON idx.indrelid = c.oid JOIN pg_class i ON idx.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'brin' ORDER BY n.nspname, c.relname LIMIT %(limit)s;")
        adoc_content.append("SELECT n.nspname||'.'||c.relname AS table_name, s.indexrelname AS index_name, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch FROM pg_stat_user_indexes s JOIN pg_class c ON s.relid = c.oid JOIN pg_class i ON s.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'brin' ORDER BY s.idx_scan DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "BRIN Index Details", 
            "SELECT n.nspname||'.'||c.relname AS table_name, i.relname AS index_name, am.amname AS index_type, pg_size_pretty(pg_relation_size(i.oid)) AS index_size FROM pg_index idx JOIN pg_class c ON idx.indrelid = c.oid JOIN pg_class i ON idx.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'brin' ORDER BY n.nspname, c.relname LIMIT %(limit)s;", 
            True,
            "brin_index_details" # Data key
        ),
        (
            "BRIN Index Usage Statistics", 
            "SELECT n.nspname||'.'||c.relname AS table_name, s.indexrelname AS index_name, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch FROM pg_stat_user_indexes s JOIN pg_class c ON s.relid = c.oid JOIN pg_class i ON s.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'brin' ORDER BY s.idx_scan DESC LIMIT %(limit)s;", 
            True,
            "brin_index_usage_statistics" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
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
    
    adoc_content.append("[TIP]\n====\nBRIN indexes are efficient for large, monotonically increasing data (e.g., timestamps). Ensure they are used for appropriate workloads. Low idx_scan values may indicate underutilized indexes; consider replacing with B-tree indexes if range queries are frequent. For Aurora, monitor index performance via CloudWatch metrics.\n====")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora supports BRIN indexes, but their effectiveness depends on workload. Use CloudWatch to monitor query performance and IOPS.\n====")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

