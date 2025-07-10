def run_cache_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes PostgreSQL buffer cache usage and hit ratios to identify performance bottlenecks.
    """
    adoc_content = ["=== Cache Analysis", "Analyzes PostgreSQL buffer cache usage and hit ratios to identify performance bottlenecks.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Get PostgreSQL version
    pg_version_query = "SHOW server_version_num;"
    _, raw_pg_version = execute_query(pg_version_query, is_check=True, return_raw=True)
    pg_version_num = int(raw_pg_version) # e.g., 170000 for PG 17

    # Determine if it's PostgreSQL 17 or newer (version number 170000 and above)
    is_pg17_or_newer = pg_version_num >= 170000

    if settings['show_qry'] == 'true':
        adoc_content.append("Cache analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT datname, blks_hit, blks_read, round((blks_hit::float / (blks_hit + blks_read) * 100)::numeric, 2) AS hit_ratio_percent FROM pg_stat_database WHERE blks_read > 0 AND datname = %(database)s;")
        
        if is_pg17_or_newer:
            # For PG17+, pg_stat_bgwriter has changed. Checkpoint stats are in pg_stat_checkpointer.
            # We'll select relevant buffer stats from pg_stat_bgwriter and checkpoint stats from pg_stat_checkpointer
            # For simplicity in a single query block, we'll focus on the bgwriter buffer stats here.
            # The checkpoint module will handle detailed checkpoint stats.
            adoc_content.append("SELECT buffers_alloc, buffers_clean FROM pg_stat_bgwriter;")
            adoc_content.append("SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, buffers_written AS buffers_checkpoint FROM pg_stat_checkpointer;")
        else:
            adoc_content.append("SELECT buffers_alloc, buffers_backend, buffers_clean, buffers_checkpoint, checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter;")
        adoc_content.append("----")

    queries = [
        (
            "Database Cache Hit Ratio", 
            "SELECT datname, blks_hit, blks_read, round((blks_hit::float / (blks_hit + blks_read) * 100)::numeric, 2) AS hit_ratio_percent FROM pg_stat_database WHERE blks_read > 0 AND datname = %(database)s;", 
            True, 
            "database_cache_hit_ratio" # Data key
        )
    ]

    # Add Buffer Cache Statistics based on PG version
    if is_pg17_or_newer:
        queries.append(
            (
                "Background Writer Buffer Statistics (PostgreSQL 17+)", 
                "SELECT buffers_alloc, buffers_clean FROM pg_stat_bgwriter;", 
                True, 
                "bgwriter_buffer_statistics" # Data key for bgwriter specific buffer stats
            )
        )
        queries.append(
            (
                "Checkpoint Buffer Statistics (PostgreSQL 17+)", 
                "SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, buffers_written AS buffers_checkpoint FROM pg_stat_checkpointer;", 
                True, 
                "checkpoint_buffer_statistics" # Data key for checkpoint specific buffer stats
            )
        )
    else:
        queries.append(
            (
                "Buffer Cache Statistics (PostgreSQL < 17)", 
                "SELECT buffers_alloc, buffers_backend, buffers_clean, buffers_checkpoint, checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter;", 
                True, 
                "buffer_cache_statistics" # Data key
            )
        )

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
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
    
    adoc_content.append("[TIP]\n====\nA cache hit ratio below 90% may indicate insufficient shared_buffers or ineffective query plans. Increase shared_buffers in the RDS parameter group for Aurora or adjust queries to improve cache efficiency. High checkpoint activity suggests tuning `checkpoint_timeout` or `max_wal_size`.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nFor AWS RDS Aurora, shared_buffers and checkpoint settings are managed via the parameter group. Use AWS Console to adjust these parameters.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
