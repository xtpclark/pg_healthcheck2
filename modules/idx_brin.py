def run_brin_idx(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== BRIN Index Analysis", "Analyzes BRIN (Block Range Index) indexes to evaluate their usage and efficiency."]
    
    if settings['show_qry'] == 'true':
        content.append("BRIN index queries:")
        content.append("[,sql]\n----")
        content.append("SELECT n.nspname||'.'||c.relname AS table_name, i.relname AS index_name, am.amname AS index_type, pg_size_pretty(pg_relation_size(i.oid)) AS index_size FROM pg_index idx JOIN pg_class c ON idx.indrelid = c.oid JOIN pg_class i ON idx.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'brin' ORDER BY n.nspname, c.relname LIMIT %(limit)s;")
        content.append("SELECT n.nspname||'.'||c.relname AS table_name, s.indexrelname AS index_name, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch FROM pg_stat_user_indexes s JOIN pg_class c ON s.relid = c.oid JOIN pg_class i ON s.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'brin' ORDER BY s.idx_scan DESC LIMIT %(limit)s;")
        content.append("----")

    queries = [
        ("BRIN Index Details", "SELECT n.nspname||'.'||c.relname AS table_name, i.relname AS index_name, am.amname AS index_type, pg_size_pretty(pg_relation_size(i.oid)) AS index_size FROM pg_index idx JOIN pg_class c ON idx.indrelid = c.oid JOIN pg_class i ON idx.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'brin' ORDER BY n.nspname, c.relname LIMIT %(limit)s;", True),
        ("BRIN Index Usage Statistics", "SELECT n.nspname||'.'||c.relname AS table_name, s.indexrelname AS index_name, s.idx_scan, s.idx_tup_read, s.idx_tup_fetch FROM pg_stat_user_indexes s JOIN pg_class c ON s.relid = c.oid JOIN pg_class i ON s.indexrelid = i.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_am am ON i.relam = am.oid WHERE am.amname = 'brin' ORDER BY s.idx_scan DESC LIMIT %(limit)s;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        params = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        result = execute_query(query, params=params)
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\nBRIN indexes are efficient for large, monotonically increasing data (e.g., timestamps). Ensure they are used for appropriate workloads. Low idx_scan values may indicate underutilized indexes; consider replacing with B-tree indexes if range queries are frequent. For Aurora, monitor index performance via CloudWatch metrics.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora supports BRIN indexes, but their effectiveness depends on workload. Use CloudWatch to monitor query performance and IOPS.\n====")
    
    return "\n".join(content)
