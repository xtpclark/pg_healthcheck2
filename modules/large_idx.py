def run_large_idx(cursor, settings, execute_query, execute_pgbouncer):
    """
    Identifies the largest indexes in the PostgreSQL database,
    which can consume significant storage and impact backup/restore times.
    """
    content = ["=== Largest Indexes", "Identifies the largest indexes in the PostgreSQL database."]
    
    if settings['show_qry'] == 'true':
        content.append("Largest indexes query:")
        content.append("[,sql]\n----")
        content.append("SELECT relname as table_name, pg_size_pretty(pg_total_relation_size(relid)) As \"Total Size\", pg_size_pretty(pg_indexes_size(relid)) as \"Index Size\", pg_size_pretty(pg_relation_size(relid)) as \"Actual Size\" FROM pg_catalog.pg_statio_user_tables ORDER BY pg_indexes_size(relid) DESC LIMIT %(limit)s;")
        content.append("----")

    main_query = "SELECT relname as table_name, pg_size_pretty(pg_total_relation_size(relid)) As \"Total Size\", pg_size_pretty(pg_indexes_size(relid)) as \"Index Size\", pg_size_pretty(pg_relation_size(relid)) as \"Actual Size\" FROM pg_catalog.pg_statio_user_tables ORDER BY pg_indexes_size(relid) DESC LIMIT %(limit)s;"
    params_for_query = {'limit': settings['row_limit']}
    result = execute_query(main_query, params=params_for_query)
    
    if "[ERROR]" in result or "[NOTE]" in result:
        content.append(result)
    else:
        content.append(result)
    
    content.append("[TIP]\n====\n"
                   "Large indexes consume significant disk space and can impact the performance of `INSERT`, `UPDATE`, and `DELETE` operations. "
                   "They also increase backup and restore times. "
                   "Review the necessity of very large indexes; consider partial indexes or re-evaluating indexing strategies if they are excessively large relative to the table size.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "While AWS RDS Aurora handles storage scaling, large indexes still contribute to `IOPS` and can impact backup/restore efficiency. "
                       "Ensure index sizes are reasonable for your workload and prune unnecessary indexes to optimize resource usage.\n"
                       "====\n")
    
    return "\n".join(content)

