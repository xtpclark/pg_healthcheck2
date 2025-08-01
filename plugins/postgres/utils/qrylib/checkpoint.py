"""
Query library for the checkpoint check.
"""

def get_checkpoint_query(connector):
    """
    Returns the appropriate query for checkpoint analysis based on PG version.
    Checkpoint stats moved from pg_stat_bgwriter to pg_stat_checkpointer in PG17.
    """
    if connector.version_info.get('is_pg17_or_newer'):
        # PostgreSQL 17 and newer query
        return """
            SELECT
                num_timed AS checkpoints_timed,
                num_requested AS checkpoints_req,
                write_time AS checkpoint_write_time,
                sync_time AS checkpoint_sync_time,
                (num_timed + num_requested) as total_checkpoints
            FROM pg_stat_checkpointer;
        """
    elif connector.version_info.get('is_pg15_or_newer'):
        # PostgreSQL 15 and 16 query
        return """
            SELECT
                checkpoints_timed,
                checkpoints_req,
                checkpoint_write_time,
                checkpoint_sync_time,
                (checkpoints_timed + checkpoints_req) as total_checkpoints
            FROM pg_stat_bgwriter;
        """
    else:
        # Query for versions older than 15
        return """
            SELECT
                checkpoints_timed,
                checkpoints_req,
                checkpoint_write_time,
                checkpoint_sync_time,
                (checkpoints_timed + checkpoints_req) as total_checkpoints
            FROM pg_stat_bgwriter;
        """
