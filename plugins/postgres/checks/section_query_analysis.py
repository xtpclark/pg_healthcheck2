def run_query_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes query performance, execution times, and query patterns to identify optimization opportunities.
    """
    adoc_content = ["Analyzes query performance, execution times, and query patterns to identify optimization opportunities."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Import version compatibility module
    from .postgresql_version_compatibility import get_postgresql_version, get_pg_stat_statements_query, validate_postgresql_version
    
    # Get PostgreSQL version compatibility information
    compatibility = get_postgresql_version(cursor, execute_query)
    
    # Validate PostgreSQL version
    is_supported, error_msg = validate_postgresql_version(compatibility)
    if not is_supported:
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        return "\n".join(adoc_content)
    
    # Get version-specific pg_stat_statements query
    pg_stat_query = get_pg_stat_statements_query(compatibility, 'standard')
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Query analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(f"{pg_stat_query} LIMIT %(limit)s;")
        adoc_content.append("SELECT state, count(*) AS query_count FROM pg_stat_activity WHERE datname = %(database)s AND query NOT LIKE 'autovacuum:%' GROUP BY state;")
        adoc_content.append("----")

    queries = [
        ("Top Queries by Execution Time", f"{pg_stat_query} LIMIT %(limit)s;", settings['has_pgstat'] == 't'),
        ("Active Query States", "SELECT state, count(*) AS query_count FROM pg_stat_activity WHERE datname = %(database)s AND query NOT LIKE 'autovacuum:%' GROUP BY state;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\n{'pg_stat_statements not installed.' if 'pg_stat_statements' in query else 'Query not applicable.'}\n====")
            continue
        params = {'database': settings['database'], 'limit': settings['row_limit']} if '%(' in query else None
        result = execute_query(query, params=params)
        if "[ERROR]" in result or "[NOTE]" in result:
            adoc_content.append(f"{title}\n{result}")
        else:
            adoc_content.append(title)
            adoc_content.append(result)
    
    adoc_content.append("[TIP]\n====\nHigh total_exec_time or mean_exec_time indicates resource-intensive queries; optimize them using indexes or query rewriting. Monitor active query counts to detect contention. For Aurora, use CloudWatch to track query performance and CPUUsage.\n====")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora integrates with CloudWatch for query performance monitoring. Enable pg_stat_statements for detailed query metrics.\n====")
    
    return "\n".join(adoc_content)
