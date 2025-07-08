def run_hot_queries(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies "Hot Queries" (most frequently executed queries) from pg_stat_statements,
    analyzing their call frequency and average execution time to pinpoint
    potential performance bottlenecks due to high volume.
    """
    adoc_content = ["=== Hot Queries", "Identifies frequently executed queries to pinpoint potential performance bottlenecks due to high volume.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Explanation of Hot Queries for the report
    adoc_content.append("By querying `pg_stat_statements`, this section identifies queries that are executed most frequently (`calls`). "
                        "Even if a query is individually fast, a very high call count can lead to significant cumulative resource consumption (CPU, I/O) "
                        "and impact overall database performance. These queries could potentially impact performance and are likely to also "
                        "show up in the 'Missing Indexes' section or other analyses dealing with `pg_stat_user_tables` if they are inefficient.\n")

    # Define the query string for Hot Queries
    # Removed ::varchar(100) cast from the query column to prevent truncation
    hot_queries_query = """
        SELECT
            trim(regexp_replace(query, '\\s+',' ','g')) AS query, -- Removed ::varchar(100)
            calls,
            total_exec_time,
            CASE
                WHEN calls > 0 THEN (total_exec_time / calls)
                ELSE 0
            END AS mean_exec_time_ms, -- Renamed for clarity and consistency
            shared_blks_hit,
            shared_blks_read
        FROM pg_stat_statements
        WHERE calls > 0
        ORDER BY calls DESC LIMIT %(limit)s;
    """

    if settings['show_qry'] == 'true':
        adoc_content.append("Hot Queries query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(hot_queries_query)
        adoc_content.append("----")

    # Check condition for pg_stat_statements
    condition = settings['has_pgstat'] == 't'

    if not condition:
        note_msg = "pg_stat_statements extension is not installed or enabled. Install pg_stat_statements to analyze hot queries."
        adoc_content.append(f"[NOTE]\n====\n{note_msg}\n====\n")
        structured_data["hot_queries"] = {"status": "not_applicable", "reason": note_msg}
    else:
        # Standardized parameter passing pattern:
        params_for_query = {'limit': settings['row_limit']}
        formatted_result, raw_result = execute_query(hot_queries_query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"Hot Queries\n{formatted_result}")
            structured_data["hot_queries"] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append("Hot Queries")
            adoc_content.append(formatted_result)
            structured_data["hot_queries"] = {"status": "success", "data": raw_result}
    
    adoc_content.append("[TIP]\n====\n"
                   "Focus on queries with a high `calls` count. Even if their `mean_exec_time_ms` is low, their cumulative impact can be significant. "
                   "Investigate these queries for potential micro-optimizations, such as reducing redundant executions, optimizing caching at the application level, "
                   "or ensuring they are fully covered by appropriate indexes. "
                   "High `shared_blks_read` for hot queries indicates frequent disk I/O, which can be a bottleneck. "
                   "For Aurora, highly frequent queries directly contribute to `CPUUtilization` and `IOPS`.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora's architecture is designed for high throughput, but extremely hot queries can still saturate instance resources. "
                       "Use Amazon RDS Performance Insights to visualize the impact of these high-frequency queries on database load. "
                       "Ensure `pg_stat_statements.track` is set to `all` for comprehensive data collection.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
