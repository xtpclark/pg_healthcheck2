"""
Version-aware queries for the Core Monitoring Metrics check.
"""

def get_database_activity_stats_query(connector):
    """
    Returns the query for basic database activity statistics.
    The columns are stable across recent PG versions.
    """
    return """
        SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
               tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted
        FROM pg_stat_database WHERE datname = %(database)s;
    """

def get_overall_transaction_buffer_stats_query(connector):
    """
    Returns the query for system-wide transaction and connection counts.
    """
    return """
        SELECT sum(numbackends) AS total_connections,
               sum(xact_commit) AS total_commits,
               sum(xact_rollback) AS total_rollbacks
        FROM pg_stat_database;
    """

def get_bgwriter_checkpoint_summary_query(connector):
    """
    Returns a version-aware query for bgwriter and checkpoint statistics,
    handling the view changes in PostgreSQL 17+.
    """
    if connector.version_info.get('is_pg17_or_newer'):
        # In PG17+, checkpoint stats are in their own view.
        return """
            SELECT num_timed AS checkpoints_timed,
                   num_requested AS checkpoints_req,
                   write_time AS checkpoint_write_time,
                   sync_time AS checkpoint_sync_time,
                   buffers_written AS buffers_checkpoint
            FROM pg_stat_checkpointer;
        """
    else:
        # For older versions, all stats are in pg_stat_bgwriter.
        return """
            SELECT checkpoints_timed, checkpoints_req,
                   buffers_alloc, buffers_clean, buffers_backend,
                   buffers_checkpoint, buffers_backend_fsync
            FROM pg_stat_bgwriter;
        """
