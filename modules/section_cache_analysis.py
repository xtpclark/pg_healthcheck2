def run_cache_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes PostgreSQL buffer cache usage and hit ratios to identify performance bottlenecks.
    """
    adoc_content = ["Analyzes PostgreSQL buffer cache usage and hit ratios to identify performance bottlenecks.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Cache analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT datname, blks_hit, blks_read, round((blks_hit::float / (blks_hit + blks_read) * 100)::numeric, 2) AS hit_ratio_percent FROM pg_stat_database WHERE blks_read > 0 AND datname = %(database)s;")
        adoc_content.append("SELECT buffers_alloc, buffers_backend, buffers_clean, buffers_checkpoint, checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter;")
        adoc_content.append("----")

    queries = [
        (
            "Database Cache Hit Ratio", 
            "SELECT datname, blks_hit, blks_read, round((blks_hit::float / (blks_hit + blks_read) * 100)::numeric, 2) AS hit_ratio_percent FROM pg_stat_database WHERE blks_read > 0 AND datname = %(database)s;", 
            True, 
            "database_cache_hit_ratio" # Data key
        ),
        (
            "Buffer Cache Statistics", 
            "SELECT buffers_alloc, buffers_backend, buffers_clean, buffers_checkpoint, checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter;", 
            True, 
            "buffer_cache_statistics" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        # Check if the query uses %(database)s or %(limit)s
        params_for_query = None
        if '%(database)s' in query:
            params_for_query = {'database': settings['database']}
        if '%(limit)s' in query and params_for_query is None:
            params_for_query = {'limit': settings['row_limit']}
        elif '%(limit)s' in query and params_for_query is not None:
            params_for_query['limit'] = settings['row_limit']

        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nA cache hit ratio below 90% may indicate insufficient shared_buffers or ineffective query plans. Increase shared_buffers in the RDS parameter group for Aurora or adjust queries to improve cache efficiency. High checkpoints_req values suggest tuning checkpoint_timeout or max_wal_size.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nFor AWS RDS Aurora, shared_buffers and checkpoint settings are managed via the parameter group. Use AWS Console to adjust these parameters.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

