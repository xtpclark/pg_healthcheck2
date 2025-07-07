def run_large_tbl(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Large Tables Analysis", "Identifies large tables by size to assess storage and performance impact."]
    
    if settings['show_qry'] == 'true':
        content.append("Large tables queries:")
        content.append("[,sql]\n----")
        content.append("SELECT n.nspname||'.'||c.relname AS table_name, pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size, pg_size_pretty(pg_table_size(c.oid)) AS table_size, pg_size_pretty(pg_indexes_size(c.oid)) AS index_size, c.reltuples AS estimated_rows FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'r' ORDER BY pg_total_relation_size(c.oid) DESC LIMIT %(limit)s;")
        content.append("----")

    queries = [
        ("Large Tables", "SELECT n.nspname||'.'||c.relname AS table_name, pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size, pg_size_pretty(pg_table_size(c.oid)) AS table_size, pg_size_pretty(pg_indexes_size(c.oid)) AS index_size, c.reltuples AS estimated_rows FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'r' ORDER BY pg_total_relation_size(c.oid) DESC LIMIT %(limit)s;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        params = {'limit': settings['row_limit']}
        result = execute_query(query, params=params)
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\nLarge tables with high index sizes may contribute to I/O and CPU load. Consider partitioning large tables or optimizing indexes. For Aurora, monitor IOPS and storage usage via CloudWatch to manage performance.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora scales storage automatically, but large tables can still impact performance. Use CloudWatch to monitor IOPS and storage metrics.\n====")
    
    return "\n".join(content)
