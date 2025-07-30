def run_matview(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes PostgreSQL materialized views, including their size, last refresh time,
    and refresh method, to optimize data warehousing and reporting.
    """
    adoc_content = ["=== Materialized View Analysis", "Analyzes materialized views for size, refresh status, and potential optimization opportunities."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Materialized view queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT schemaname, matviewname, pg_size_pretty(pg_relation_size(matviewname::regclass)) AS size, ispopulated FROM pg_matviews ORDER BY size DESC LIMIT %(limit)s;")
        adoc_content.append("SELECT mvname, age(relfrozenxid) AS xid_age, last_refresh FROM pg_stat_matviews ORDER BY xid_age DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    # Check if pg_stat_matviews exists before attempting to query it
    chk_pg_stat_matviews_query = "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'pg_stat_matviews' AND relkind = 'v');"
    # Execute with return_raw=True to get the boolean result directly
    pg_stat_matviews_exists_formatted, pg_stat_matviews_exists_raw = execute_query(chk_pg_stat_matviews_query, is_check=True, return_raw=True)
    
    queries = [
        (
            "Materialized View Sizes and Population Status", 
            "SELECT schemaname, matviewname, pg_size_pretty(pg_relation_size(matviewname::regclass)) AS size, ispopulated FROM pg_matviews ORDER BY size DESC LIMIT %(limit)s;", 
            True,
            "matview_sizes_population" # Data key
        ),
        (
            "Materialized View XID Age and Last Refresh", 
            "SELECT mvname, age(relfrozenxid) AS xid_age, last_refresh FROM pg_stat_matviews ORDER BY xid_age DESC LIMIT %(limit)s;", 
            pg_stat_matviews_exists_raw == 't', # Condition based on raw existence check
            "matview_xid_age_last_refresh" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            # Provide a more specific note if pg_stat_matviews is missing
            note_msg = "Query not applicable."
            if "pg_stat_matviews" in query and pg_stat_matviews_exists_raw == 'f':
                note_msg = "Query not applicable. The 'pg_stat_matviews' view does not exist in this PostgreSQL version or is not accessible. This view is typically available in PostgreSQL 9.4 and later."
            
            adoc_content.append(f"{title}\n[NOTE]\n====\n{note_msg}\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": note_msg}
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
                   "Monitor materialized view sizes and ensure they are regularly refreshed to reflect current data. "
                   "High XID age on materialized views can indicate a need for more frequent refreshes or `VACUUM FREEZE` if not refreshed concurrently. "
                   "Consider `REFRESH MATERIALIZED VIEW CONCURRENTLY` for large views to minimize downtime. "
                   "For Aurora, materialized views behave similarly, and their refresh strategy should be optimized for performance and data freshness.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora supports materialized views. Performance of `REFRESH MATERIALIZED VIEW` operations will depend on instance resources and I/O. "
                       "Monitor CPU and IOPS during refresh operations via CloudWatch.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

