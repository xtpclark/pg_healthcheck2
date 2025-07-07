def run_matview(cursor, settings, execute_query, execute_pgbouncer):
    """
    Analyzes PostgreSQL materialized views, including their size, last refresh time,
    and refresh method, to optimize data warehousing and reporting.
    """
    content = ["=== Materialized View Analysis", "Analyzes materialized views for size, refresh status, and potential optimization opportunities."]
    
    if settings['show_qry'] == 'true':
        content.append("Materialized view queries:")
        content.append("[,sql]\n----")
        content.append("SELECT schemaname, matviewname, pg_size_pretty(pg_relation_size(matviewname::regclass)) AS size, ispopulated FROM pg_matviews ORDER BY size DESC LIMIT %(limit)s;")
        content.append("SELECT mvname, age(relfrozenxid) AS xid_age, last_refresh FROM pg_stat_matviews ORDER BY xid_age DESC LIMIT %(limit)s;")
        content.append("----")

    # Check if pg_stat_matviews exists before attempting to query it
    chk_pg_stat_matviews_query = "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'pg_stat_matviews' AND relkind = 'v');"
    pg_stat_matviews_exists = execute_query(chk_pg_stat_matviews_query, is_check=True)
    
    queries = [
        (
            "Materialized View Sizes and Population Status", 
            "SELECT schemaname, matviewname, pg_size_pretty(pg_relation_size(matviewname::regclass)) AS size, ispopulated FROM pg_matviews ORDER BY size DESC LIMIT %(limit)s;", 
            True
        ),
        (
            "Materialized View XID Age and Last Refresh", 
            "SELECT mvname, age(relfrozenxid) AS xid_age, last_refresh FROM pg_stat_matviews ORDER BY xid_age DESC LIMIT %(limit)s;", 
            pg_stat_matviews_exists == 't' # Condition based on existence check
        )
    ]

    for title, query, condition in queries:
        if not condition:
            # Provide a more specific note if pg_stat_matviews is missing
            if "pg_stat_matviews" in query and pg_stat_matviews_exists == 'f':
                content.append(f"{title}\n[NOTE]\n====\nQuery not applicable. The 'pg_stat_matviews' view does not exist in this PostgreSQL version or is not accessible. This view is typically available in PostgreSQL 9.4 and later.\n====\n")
            else:
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
                   "Monitor materialized view sizes and ensure they are regularly refreshed to reflect current data. "
                   "High XID age on materialized views can indicate a need for more frequent refreshes or `VACUUM FREEZE` if not refreshed concurrently. "
                   "Consider `REFRESH MATERIALIZED VIEW CONCURRENTLY` for large views to minimize downtime. "
                   "For Aurora, materialized views behave similarly, and their refresh strategy should be optimized for performance and data freshness.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora supports materialized views. Performance of `REFRESH MATERIALIZED VIEW` operations will depend on instance resources and I/O. "
                       "Monitor CPU and IOPS during refresh operations via CloudWatch.\n"
                       "====\n")
    
    return "\n".join(content)

