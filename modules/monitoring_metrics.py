def run_monitoring_metrics(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Gathers general PostgreSQL monitoring metrics from various pg_stat_ views
    to provide insights into overall database performance.
    """
    adoc_content = ["=== General Monitoring Metrics", "Gathers key performance metrics for overall database health monitoring."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Get PostgreSQL version
    pg_version_query = "SHOW server_version_num;"
    _, raw_pg_version = execute_query(pg_version_query, is_check=True, return_raw=True)
    pg_version_num = int(raw_pg_version) # e.g., 170000 for PG 17

    # Determine if it's PostgreSQL 17 or newer (version number 170000 and above)
    is_pg17_or_newer = pg_version_num >= 170000

    if settings['show_qry'] == 'true':
        adoc_content.append("General monitoring metrics queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted FROM pg_stat_database WHERE datname = %(database)s;")
        adoc_content.append("SELECT sum(numbackends) AS total_connections, sum(xact_commit) AS total_commits, sum(xact_rollback) AS total_rollbacks FROM pg_stat_database;")
        
        if is_pg17_or_newer:
            # For PG17+, query pg_stat_checkpointer for checkpoint-related stats
            adoc_content.append("SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, write_time AS checkpoint_write_time, sync_time AS checkpoint_sync_time, buffers_written AS buffers_checkpoint FROM pg_stat_checkpointer;")
        else:
            # For older PG versions, query pg_stat_bgwriter
            adoc_content.append("SELECT checkpoints_timed, checkpoints_req, buffers_alloc, buffers_clean, buffers_backend, buffers_checkpoint, buffers_backend_fsync FROM pg_stat_bgwriter;")
        adoc_content.append("----")

    queries = [
        (
            "Database Activity Statistics", 
            "SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted FROM pg_stat_database WHERE datname = %(database)s;", 
            True,
            "database_activity_stats" # Data key
        ),
        (
            "Overall Database Transaction & Buffer Stats", 
            "SELECT sum(numbackends) AS total_connections, sum(xact_commit) AS total_commits, sum(xact_rollback) AS total_rollbacks FROM pg_stat_database;", 
            True,
            "overall_transaction_buffer_stats" # Data key
        )
    ]

    # Add Background Writer & Checkpoint Summary based on PG version
    if is_pg17_or_newer:
        queries.append(
            (
                "Background Writer & Checkpoint Summary (PostgreSQL 17+)", 
                "SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, write_time AS checkpoint_write_time, sync_time AS checkpoint_sync_time, buffers_written AS buffers_checkpoint FROM pg_stat_checkpointer;", 
                True,
                "bgwriter_checkpoint_summary" # Data key
            )
        )
    else:
        queries.append(
            (
                "Background Writer & Checkpoint Summary (PostgreSQL < 17)", 
                "SELECT checkpoints_timed, checkpoints_req, buffers_alloc, buffers_clean, buffers_backend, buffers_checkpoint, buffers_backend_fsync FROM pg_stat_bgwriter;", 
                True,
                "bgwriter_checkpoint_summary" # Data key
            )
        )


    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {'database': settings['database']} if '%(database)s' in query else None
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\n"
                   "Regularly monitoring these general metrics provides a high-level view of database activity. "
                   "High `xact_rollback` counts can indicate application errors or contention. "
                   "Compare `blks_read` vs `blks_hit` to understand cache efficiency. "
                   "For Aurora, these metrics complement CloudWatch data and help pinpoint database-internal performance characteristics.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora provides many of these metrics via CloudWatch (e.g., `DatabaseConnections`, `CommitLatency`, `RollbackLatency`, `BufferCacheHitRatio`). "
                       "Use these PostgreSQL internal views for more granular details within the database instance itself.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
