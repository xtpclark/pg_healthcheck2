def run_unused_idx(cursor, settings, execute_query, execute_pgbouncer):
    """
    Identifies unused indexes in the PostgreSQL database.
    Unused indexes can incur write overhead and storage costs without providing query benefits.
    """
    content = ["=== Unused Indexes", "Identifies unused indexes in the PostgreSQL database."]
    
    if settings['show_qry'] == 'true':
        content.append("Unused indexes query:")
        content.append("[,sql]\n----")
        content.append("SELECT schemaname||'.'||relname AS schemarelname, indexrelname FROM pg_stat_user_indexes WHERE idx_scan = 0 ORDER BY 1 LIMIT %(limit)s;")
        content.append("----")

    # Condition to check if any unused indexes exist
    condition_query = "SELECT EXISTS (SELECT 1 FROM pg_stat_user_indexes WHERE idx_scan = 0);"
    chk_result = execute_query(condition_query, is_check=True)

    if chk_result == 'f':
        content.append("[NOTE]\n====\nNo unused indexes found.\n====\n")
    else:
        main_query = "SELECT schemaname||'.'||relname AS schemarelname, indexrelname FROM pg_stat_user_indexes WHERE idx_scan = 0 ORDER BY 1 LIMIT %(limit)s;"
        params_for_query = {'limit': settings['row_limit']}
        result = execute_query(main_query, params=params_for_query)
        
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(result)
        else:
            content.append(result)
            content.append("Unused indexes were found. Since indexes can add significant overhead to any table change operation, they should be removed if they are not being used for either queries or constraint enforcement (such as making sure a value is unique).\n")
    
    content.append("[TIP]\n====\n"
                   "Regularly review `pg_stat_user_indexes` for indexes with `idx_scan = 0`. "
                   "These indexes consume disk space and add overhead to `INSERT`, `UPDATE`, and `DELETE` operations without providing query performance benefits. "
                   "Consider dropping unused indexes after thorough analysis and testing.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, unused indexes contribute to storage costs and write amplification. "
                       "Removing them can reduce `WriteIOPS` and improve overall database performance. "
                       "Always test index removal in a staging environment first.\n"
                       "====\n")
    
    return "\n".join(content)

