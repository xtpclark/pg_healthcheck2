def run_table_metrics(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes table sizes and live/dead tuples to identify potential issues.
    """
    adoc_content = ["Analyzes table sizes and live/dead tuples to identify potential issues.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module

    if settings['show_qry'] == 'true':
        adoc_content.append("Table metrics queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT schemaname||'.'||relname AS table_name, pg_size_pretty(pg_total_relation_size(relid)) AS size FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT %(limit)s;")
        adoc_content.append("SELECT relname, n_live_tup AS live_tuples, n_dead_tup AS dead_tuples FROM pg_stat_user_tables WHERE n_dead_tup > 0 ORDER BY n_dead_tup DESC LIMIT %(limit)s;")
        adoc_content.append("SELECT relname, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze FROM pg_stat_user_tables WHERE last_autovacuum IS NOT NULL ORDER BY last_autovacuum DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "Table Sizes", 
            "SELECT schemaname||'.'||relname AS table_name, pg_size_pretty(pg_total_relation_size(relid)) AS size FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT %(limit)s;",
            True,
            "table_sizes" # Data key
        ),
        (
            "Live and Dead Tuples", 
            "SELECT relname, n_live_tup AS live_tuples, n_dead_tup AS dead_tuples FROM pg_stat_user_tables WHERE n_dead_tup > 0 ORDER BY n_dead_tup DESC LIMIT %(limit)s;",
            True,
            "live_dead_tuples" # Data key
        ),
        (
            "Vacuum and Analyze Status", 
            "SELECT relname, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze FROM pg_stat_user_tables WHERE last_autovacuum IS NOT NULL ORDER BY last_autovacuum DESC LIMIT %(limit)s;",
            True,
            "vacuum_analyze_status" # Data key
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
    
    adoc_content.append("[TIP]\n====\nHigh dead tuples may indicate autovacuum tuning needs. Run VACUUM ANALYZE on large tables.\n====")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
