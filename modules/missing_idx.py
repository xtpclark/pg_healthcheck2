def run_missing_idx(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies tables with potentially missing indexes, based on high sequential scans
    and low or no index scans.
    """
    adoc_content = ["=== Tables with Potentially Missing Indexes", "Identifies tables with high sequential scans and low or no index scans, indicating potential missing indexes."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Missing index query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT schemaname||'.'||relname AS schemarelname, seq_tup_read as rows_read, n_live_tup as rows_estimated, seq_scan as seq_scans from pg_stat_user_tables where (idx_scan = 0 or idx_scan is null) ORDER BY 2 DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    # Condition to check if any potentially missing indexes exist
    condition_query = "SELECT EXISTS (SELECT 1 from pg_stat_user_tables where (idx_scan = 0 or idx_scan is null));"
    chk_result_formatted, chk_result_raw = execute_query(condition_query, is_check=True, return_raw=True)

    # Main query for potentially missing indexes
    main_query = "SELECT schemaname||'.'||relname AS schemarelname, seq_tup_read as rows_read, n_live_tup as rows_estimated, seq_scan as seq_scans from pg_stat_user_tables where (idx_scan = 0 or idx_scan is null) ORDER BY 2 DESC LIMIT %(limit)s;"
    params_for_query = {'limit': settings['row_limit']}

    if chk_result_raw == 'f': # Use the raw boolean result for the condition check
        adoc_content.append("[NOTE]\n====\nGreat! No potentially missing indexes found.\n====\n")
        structured_data["missing_indexes"] = {"status": "success", "data": []}
    else:
        formatted_result, raw_result = execute_query(main_query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["missing_indexes"] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(formatted_result)
            adoc_content.append("[IMPORTANT]\n====\nWe might have some missing indexes...\n\n. The name of the table (schemarelname) including the schemaname.\n. How often our table has been read sequentially (seq_scan).\n. How often an index has been used is NOT shown... since it's 0 or null.\n. The most important information is rows_read. It tells us how many rows the system had to process to satisfy all those sequential scans.\n====\n")
            structured_data["missing_indexes"] = {"status": "success", "data": raw_result}
    
    adoc_content.append("[TIP]\n====\n"
                   "Tables with high `seq_scan` counts and low `idx_scan` counts are strong candidates for new indexes. "
                   "Focus on tables where `rows_read` is high, as this indicates significant data scanning. "
                   "Proper indexing can drastically reduce I/O and CPU usage for read-heavy queries.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, missing indexes can lead to high `CPUUtilization` and `ReadIOPS` due to full table scans. "
                       "Analyze query patterns (e.g., using `pg_stat_statements`) to identify columns frequently used in `WHERE` clauses, `JOIN` conditions, and `ORDER BY` clauses that could benefit from indexing.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
