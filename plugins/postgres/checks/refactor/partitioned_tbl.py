def run_list_part(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes PostgreSQL partitioned tables, listing parent tables and their partitions,
    along with their sizes, to help manage large datasets.
    """
    adoc_content = ["=== Partitioned Tables Analysis", "Identifies and analyzes partitioned tables and their individual partitions."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Partitioned tables queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT relname, relkind FROM pg_class WHERE relkind = 'p' ORDER BY relname LIMIT %(limit)s;")
        adoc_content.append("SELECT relname AS partition_name, pg_size_pretty(pg_relation_size(oid)) AS size FROM pg_class WHERE relispartition = true ORDER BY pg_relation_size(oid) DESC LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "List Partitioned Tables (Parents)", 
            "SELECT relname, relkind FROM pg_class WHERE relkind = 'p' ORDER BY relname LIMIT %(limit)s;", 
            True,
            "list_partitioned_tables" # Data key
        ),
        (
            "List Individual Partitions and Their Sizes", 
            "SELECT relname AS partition_name, pg_size_pretty(pg_relation_size(oid)) AS size FROM pg_class WHERE relispartition = true ORDER BY pg_relation_size(oid) DESC LIMIT %(limit)s;", 
            True,
            "list_individual_partitions" # Data key
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
                   "Partitioned tables are essential for managing large datasets, improving query performance, and simplifying data retention. "
                   "Ensure your partitioning strategy aligns with your data access patterns. "
                   "Monitor individual partition sizes to identify skew or unexpected growth. "
                   "For Aurora, partitioning can significantly aid in managing large tables and improving query performance.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora supports PostgreSQL's native partitioning. "
                       "Effective partitioning can reduce the amount of data scanned, leading to lower IOPS and improved CPU utilization. "
                       "Consider using partitioning for very large tables to optimize performance and maintenance.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

