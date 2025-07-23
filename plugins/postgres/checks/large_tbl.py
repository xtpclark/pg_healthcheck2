def run_large_tbl(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies large tables by size to assess storage and performance impact.
    """
    adoc_content = ["=== Large Tables Analysis", "Identifies large tables by size to assess storage and performance impact."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Large tables queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT n.nspname||'.'||c.relname AS table_name, pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size, pg_size_pretty(pg_table_size(c.oid)) AS table_size, pg_size_pretty(pg_indexes_size(c.oid)) AS index_size, c.reltuples AS estimated_rows FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'r' ORDER BY pg_total_relation_size(c.oid) DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "Large Tables", 
            "SELECT n.nspname||'.'||c.relname AS table_name, pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size, pg_size_pretty(pg_table_size(c.oid)) AS table_size, pg_size_pretty(pg_indexes_size(c.oid)) AS index_size, c.reltuples AS estimated_rows FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'r' ORDER BY pg_total_relation_size(c.oid) DESC LIMIT %(limit)s;", 
            True,
            "large_tables" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {'limit': settings['row_limit']}
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nLarge tables with high index sizes may contribute to I/O and CPU load. Consider partitioning large tables or optimizing indexes. For Aurora, monitor IOPS and storage usage via CloudWatch to manage performance.\n====")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora scales storage automatically, but large tables can still impact performance. Use CloudWatch to monitor IOPS and storage metrics.\n====")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

