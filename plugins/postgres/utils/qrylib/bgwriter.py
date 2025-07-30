def get_bgwriter_query(connector):
    """
    Returns the appropriate query for bgwriter/checkpointer analysis.
    The 'pg_stat_bgwriter' view was expanded in PostgreSQL 15 and the checkpointer
    stats were moved to their own view in 17.
    """
    if connector.version_info.get('is_pg17_or_newer'):
        return "SELECT * FROM pg_stat_checkpointer;"
    elif connector.version_info.get('is_pg15_or_newer'):
        return "SELECT *, (checkpoints_timed + checkpoints_req) as total_checkpoints FROM pg_stat_bgwriter;"
    else:
        return "SELECT *, (checkpoints_timed + checkpoints_req) as total_checkpoints FROM pg_stat_bgwriter;"

