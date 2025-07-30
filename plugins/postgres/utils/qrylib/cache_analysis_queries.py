def get_cache_analysis_queries(connector):
    """
    Returns a dictionary of queries for cache analysis, adjusted for PG version.
    Handles the split of stats from pg_stat_bgwriter to pg_stat_checkpointer in PG17+.
    """
    queries = {
        "database_cache_hit_ratio": """
            SELECT
                datname,
                blks_hit,
                blks_read,
                round((blks_hit::float / NULLIF(blks_hit + blks_read, 0) * 100)::numeric, 2) AS hit_ratio_percent
            FROM pg_stat_database
            WHERE blks_read > 0 AND datname = %(database)s;
        """
    }
    
    # In PostgreSQL 17, checkpoint stats moved from pg_stat_bgwriter to pg_stat_checkpointer.
    # We now query both to get a complete picture.
    if connector.version_info.get('is_pg17_or_newer'):
        queries["bgwriter_buffer_statistics"] = "SELECT buffers_alloc, buffers_clean FROM pg_stat_bgwriter;"
        queries["checkpoint_buffer_statistics"] = "SELECT num_timed AS checkpoints_timed, num_requested AS checkpoints_req, buffers_written AS buffers_checkpoint FROM pg_stat_checkpointer;"
    else:
        # For older versions, all stats are in pg_stat_bgwriter
        queries["buffer_cache_statistics"] = "SELECT buffers_alloc, buffers_backend, buffers_clean, buffers_checkpoint, checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter;"

    return queries

