def get_checkpoint_query(connector):
    """
    Returns the appropriate query for checkpoint analysis based on PG version.
    The 'pg_stat_bgwriter' view was expanded in PostgreSQL 15.
    """
    if connector.version_info.get('is_pg15_or_newer'):
        # PostgreSQL 15 and newer query
        return """
            SELECT
                checkpoints_timed,
                checkpoints_req,
                checkpoint_write_time,
                checkpoint_sync_time,
                checkpoint_proc_time,
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
