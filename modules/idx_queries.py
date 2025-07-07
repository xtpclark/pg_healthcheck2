def run_idx_queries(cursor, settings, execute_query, execute_pgbouncer):
    content = []
    
    # Unused Indexes
    content.append("=== Unused Indexes")
    if settings['show_qry'] == 'true':
        content.append("Unused indexes query:")
        content.append("[,sql]\n----")
        content.append("SELECT schemaname||'.'||relname AS schemarelname, indexrelname FROM pg_stat_user_indexes WHERE idx_scan = 0 ORDER BY 1 LIMIT %(limit)s;")
        content.append("----")

    chk_query = "SELECT EXISTS (SELECT 1 FROM pg_stat_user_indexes WHERE idx_scan = 0);"
    chk_result = execute_query(chk_query, is_check=True)
    if chk_result == 'f':
        content.append("No unused indexes found.")
    else:
        content.append("Unused indexes were found. Since indexes can add significant overhead to any table change operation, they should be removed if they are not being used for either queries or constraint enforcement (such as making sure a value is unique).")
        query = "SELECT schemaname||'.'||relname AS schemarelname, indexrelname FROM pg_stat_user_indexes WHERE idx_scan = 0 ORDER BY 1 LIMIT %(limit)s;"
        content.append(execute_query(query, params={'limit': settings['row_limit']}))
    
    content.append("")  # Page break equivalent
    # Duplicate Indexes
    content.append("=== Duplicate Indexes")
    if settings['show_qry'] == 'true':
        content.append("Duplicate Index query:")
        content.append("[,sql]\n----")
        content.append("SELECT pg_size_pretty(sum(pg_relation_size(idx))::bigint) AS size, (array_agg(idx))[1] AS idx1, (array_agg(idx))[2] AS idx2 FROM (SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\\n'|| indclass::text ||E'\\n'|| indkey::text ||E'\\n'|| coalesce(indexprs::text,'')||E'\\n' || coalesce(indpred::text,'')) AS KEY FROM pg_index) sub GROUP BY KEY HAVING count(*)>1 ORDER BY sum(pg_relation_size(idx)) DESC LIMIT %(limit)s;")
        content.append("----")

    chk_query = """SELECT EXISTS (SELECT 1 FROM (
                SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\\n'|| indclass::text ||E'\\n'|| indkey::text ||E'\\n'||
                        coalesce(indexprs::text,'')||E'\\n' || coalesce(indpred::text,'')) AS KEY
                        FROM pg_index) sub
                GROUP BY KEY HAVING count(*)>1
                ORDER BY sum(pg_relation_size(idx)) DESC);"""
    chk_result = execute_query(chk_query, is_check=True)
    if chk_result == 'f':
        content.append("[NOTE]\n====\nNo duplicate indexes were found.\nDuplicate or multiple indexes that have the same set of columns, same opclass, expression and predicate make them equivalent.\n====")
    else:
        content.append("[CAUTION]\n====\nDuplicate indexes were found.\nDuplicate or multiple indexes that have the same set of columns, same opclass, expression and predicate make them equivalent.\n\n. Review source code to determine where they may have originated from.\n. Understand why a duplicate might exist.\n. It may have had some purpose at some point.\n. Test thoroughly before taking any action.\n. Do not automate the task of dropping one of them.\n====")
        query = """SELECT pg_size_pretty(sum(pg_relation_size(idx))::bigint) AS size,
                (array_agg(idx))[1] AS idx1, (array_agg(idx))[2] AS idx2
                FROM (SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\\n'|| indclass::text ||E'\\n'|| indkey::text ||E'\\n'||
                        coalesce(indexprs::text,'')||E'\\n' || coalesce(indpred::text,'')) AS KEY
                        FROM pg_index) sub
                GROUP BY KEY HAVING count(*)>1
                ORDER BY sum(pg_relation_size(idx)) DESC LIMIT %(limit)s;"""
        content.append(execute_query(query, params={'limit': settings['row_limit']}))
    
    content.append("")  # Page break equivalent
    # Missing Indexes
    content.append("=== Tables with Potentially Missing Indexes")
    content.append("This query returns data calculated/collected from the pg_stat_user_tables view to determine tables which have had no index scans.\nTables which have been hit by sequential scans the most are possible candidates.")
    if settings['show_qry'] == 'true':
        content.append("Missing index query:")
        content.append("[,sql]\n----")
        content.append("SELECT schemaname||'.'||relname AS schemarelname, seq_tup_read as rows_read, n_live_tup as rows_estimated, seq_scan as seq_scans from pg_stat_user_tables where (idx_scan = 0 or idx_scan is null) ORDER BY 2 DESC LIMIT %(limit)s;")
        content.append("----")

    chk_query = "SELECT EXISTS (SELECT 1 from pg_stat_user_tables where (idx_scan = 0 or idx_scan is null));"
    chk_result = execute_query(chk_query, is_check=True)
    if chk_result == 'f':
        content.append("[NOTE]\n====\nGreat! No potentially missing indexes found.\n====")
    else:
        query = "SELECT schemaname||'.'||relname AS schemarelname, seq_tup_read as rows_read, n_live_tup as rows_estimated, seq_scan as seq_scans from pg_stat_user_tables where (idx_scan = 0 or idx_scan is null) ORDER BY 2 DESC LIMIT %(limit)s;"
        content.append(execute_query(query, params={'limit': settings['row_limit']}))
        content.append("[IMPORTANT]\n====\nWe might have some missing indexes...\n\n. The name of the table (schemarelname) including the schemaname.\n. How often our table has been read sequentially (seq_scan).\n. How often an index has been used is NOT shown... since it's 0 or null.\n. The most important information is rows_read. It tells us how many rows the system had to process to satisfy all those sequential scans.\n====")
    
    content.append("")  # Page break equivalent
    # Largest Indexes
    content.append("=== Largest Indexes")
    if settings['show_qry'] == 'true':
        content.append("Largest indexes query:")
        content.append("[,sql]\n----")
        content.append("SELECT relname as table_name, pg_size_pretty(pg_total_relation_size(relid)) As \"Total Size\", pg_size_pretty(pg_indexes_size(relid)) as \"Index Size\", pg_size_pretty(pg_relation_size(relid)) as \"Actual Size\" FROM pg_catalog.pg_statio_user_tables ORDER BY pg_indexes_size(relid) DESC LIMIT %(limit)s;")
        content.append("----")

    query = "SELECT relname as table_name, pg_size_pretty(pg_total_relation_size(relid)) As \"Total Size\", pg_size_pretty(pg_indexes_size(relid)) as \"Index Size\", pg_size_pretty(pg_relation_size(relid)) as \"Actual Size\" FROM pg_catalog.pg_statio_user_tables ORDER BY pg_indexes_size(relid) DESC LIMIT %(limit)s;"
    content.append(execute_query(query, params={'limit': settings['row_limit']}))
    
    return "\n".join(content)
