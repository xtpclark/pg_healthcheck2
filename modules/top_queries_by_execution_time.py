def run_top_queries_by_execution_time(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes top queries by total execution time from pg_stat_statements
    to identify resource-intensive queries.
    """
    adoc_content = ["=== Top Queries by Execution Time", "Identifies resource-intensive queries based on total execution time.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Import version compatibility module
    from .postgresql_version_compatibility import get_postgresql_version, get_pg_stat_statements_query, validate_postgresql_version
    
    # Get PostgreSQL version compatibility information
    compatibility = get_postgresql_version(cursor, execute_query)
    
    # Validate PostgreSQL version
    is_supported, error_msg = validate_postgresql_version(compatibility)
    if not is_supported:
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["version_error"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data
    
    # Get version-specific query
    top_queries_query = get_pg_stat_statements_query(compatibility, 'standard') + " LIMIT %(limit)s;"

    if settings['show_qry'] == 'true':
        adoc_content.append("Top queries by execution time query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(top_queries_query)
        adoc_content.append("----")

    # Check condition for pg_stat_statements
    condition = settings['has_pgstat'] == 't'

    if not condition:
        note_msg = "pg_stat_statements extension is not installed or enabled. Install pg_stat_statements to analyze top queries."
        adoc_content.append(f"[NOTE]\n====\n{note_msg}\n====\n")
        structured_data["top_queries"] = {"status": "not_applicable", "reason": note_msg}
    else:
        # Standardized parameter passing pattern:
        params_for_query = {'limit': settings['row_limit']}
        formatted_result, raw_result = execute_query(top_queries_query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"Top Queries by Execution Time\n{formatted_result}")
            structured_data["top_queries"] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append("Top Queries by Execution Time")
            adoc_content.append(formatted_result)
            structured_data["top_queries"] = {"status": "success", "data": raw_result}
    
    adoc_content.append("[TIP]\n====\n"
                   "Queries with high `total_exec_time` or `mean_exec_time` are consuming significant database resources. "
                   "Investigate these queries for optimization opportunities, such as adding appropriate indexes, rewriting inefficient parts, or adjusting application logic. "
                   "For Aurora, optimizing these queries directly reduces `CPUUtilization` and improves overall performance.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora integrates `pg_stat_statements` for detailed query performance monitoring. "
                       "Use CloudWatch to correlate high `CPUUtilization` or `DatabaseConnections` with specific long-running or frequently executed queries identified here.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
