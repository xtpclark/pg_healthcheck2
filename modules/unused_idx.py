def run_unused_idx(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies unused indexes in the PostgreSQL database.
    Unused indexes can incur write overhead and storage costs without providing query benefits.
    """
    adoc_content = ["=== Unused Indexes", "Identifies unused indexes in the PostgreSQL database."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Unused indexes query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT schemaname||'.'||relname AS schemarelname, indexrelname FROM pg_stat_user_indexes WHERE idx_scan = 0 ORDER BY 1 LIMIT %(limit)s;")
        adoc_content.append("----")

    # Condition to check if any unused indexes exist
    condition_query = "SELECT EXISTS (SELECT 1 FROM pg_stat_user_indexes WHERE idx_scan = 0);"
    chk_result_formatted, chk_result_raw = execute_query(condition_query, is_check=True, return_raw=True)
    
    # Main query for unused indexes
    main_query = "SELECT schemaname||'.'||relname AS schemarelname, indexrelname FROM pg_stat_user_indexes WHERE idx_scan = 0 ORDER BY 1 LIMIT %(limit)s;"
    params_for_query = {'limit': settings['row_limit']}

    if chk_result_raw == 'f': # Use the raw boolean result for the condition check
        adoc_content.append("[NOTE]\n====\nNo unused indexes found.\n====\n")
        structured_data["unused_indexes"] = {"status": "success", "data": []}
    else:
        formatted_result, raw_result = execute_query(main_query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["unused_indexes"] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(formatted_result)
            adoc_content.append("Unused indexes were found. Since indexes can add significant overhead to any table change operation, they should be removed if they are not being used for either queries or constraint enforcement (such as making sure a value is unique).\n")
            structured_data["unused_indexes"] = {"status": "success", "data": raw_result}
    
    adoc_content.append("[TIP]\n====\n"
                   "Regularly review `pg_stat_user_indexes` for indexes with `idx_scan = 0`. "
                   "These indexes consume disk space and add overhead to `INSERT`, `UPDATE`, and `DELETE` operations without providing query performance benefits. "
                   "Consider dropping unused indexes after thorough analysis and testing.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, unused indexes contribute to storage costs and write amplification. "
                       "Removing them can reduce `WriteIOPS` and improve overall database performance. "
                       "Always test index removal in a staging environment first.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

