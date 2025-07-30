def get_blocking_query(connector):
    """
    Returns a SQL query to identify blocking sessions, tailored to the PostgreSQL version.
    
    Args:
        connector: The database connector object with version_info attribute.
    
    Returns:
        str: The SQL query string.
    """
    version_info = connector.version_info
    major_version = version_info.get('major_version', 0)  # Default to 0 if not available
    
    if major_version >= 10:
        # Query for PostgreSQL 10 and above
        query = """
        SELECT
            blocked_locks.pid AS blocked_pid,
            blocked_activity.usename AS blocked_user,
            blocking_locks.pid AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocked_activity.query AS blocked_query,
            blocking_activity.query AS blocking_query
        FROM pg_catalog.pg_locks blocked_locks
        JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted;
        """
    else:
        # Query for older PostgreSQL versions (adjust as needed for your environment)
        query = """
        -- Simplified query for PostgreSQL versions < 10
        SELECT
            blocked.pid AS blocked_pid,
            blocked.usename AS blocked_user,
            blocking.pid AS blocking_pid,
            blocking.usename AS blocking_user,
            blocked.query AS blocked_query,
            blocking.query AS blocking_query
        FROM pg_stat_activity blocked,
             pg_stat_activity blocking
        WHERE blocked.waiting = true
          AND blocked.locked_by = blocking.pid;
        """
    return query
