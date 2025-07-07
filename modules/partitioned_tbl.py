def run_list_part(cursor, settings, execute_query, execute_pgbouncer):
    """
    Analyzes PostgreSQL partitioned tables, listing parent tables and their partitions,
    along with their sizes, to help manage large datasets.
    """
    content = ["=== Partitioned Tables Analysis", "Identifies and analyzes partitioned tables and their individual partitions."]
    
    if settings['show_qry'] == 'true':
        content.append("Partitioned tables queries:")
        content.append("[,sql]\n----")
        content.append("SELECT relname, relkind FROM pg_class WHERE relkind = 'p' ORDER BY relname LIMIT %(limit)s;")
        content.append("SELECT relname AS partition_name, pg_size_pretty(pg_relation_size(oid)) AS size FROM pg_class WHERE relispartition = true ORDER BY pg_relation_size(oid) DESC LIMIT %(limit)s;")
        content.append("----")

    queries = [
        (
            "List Partitioned Tables (Parents)", 
            "SELECT relname, relkind FROM pg_class WHERE relkind = 'p' ORDER BY relname LIMIT %(limit)s;", 
            True
        ),
        (
            "List Individual Partitions and Their Sizes", 
            "SELECT relname AS partition_name, pg_size_pretty(pg_relation_size(oid)) AS size FROM pg_class WHERE relispartition = true ORDER BY pg_relation_size(oid) DESC LIMIT %(limit)s;", 
            True
        )
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            continue
        
        # Standardized parameter passing pattern:
        # Check if the query contains the %(limit)s placeholder and pass params accordingly.
        params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        
        result = execute_query(query, params=params_for_query)
        
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\n"
                   "Partitioned tables are essential for managing large datasets, improving query performance, and simplifying data retention. "
                   "Ensure your partitioning strategy aligns with your data access patterns. "
                   "Monitor individual partition sizes to identify skew or unexpected growth. "
                   "For Aurora, partitioning can significantly aid in managing large tables and improving query performance.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora supports PostgreSQL's native partitioning. "
                       "Effective partitioning can reduce the amount of data scanned, leading to lower IOPS and improved CPU utilization. "
                       "Consider using partitioning for very large tables to optimize performance and maintenance.\n"
                       "====\n")
    
    return "\n".join(content)

