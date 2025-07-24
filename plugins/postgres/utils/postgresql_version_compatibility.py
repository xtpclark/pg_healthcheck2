"""
PostgreSQL Version Compatibility Module

This module provides version-aware functionality to construct the correct
SQL queries based on the version of the connected PostgreSQL database.
It relies on the version information fetched by the PostgresConnector.
"""

def get_pg_stat_statements_query(connector, query_type='standard'):
    """
    Get pg_stat_statements query based on PostgreSQL version.
    
    Args:
        connector (PostgresConnector): The active database connector instance.
        query_type (str): Type of query ('standard', 'write_activity', 'function_performance')
    
    Returns:
        str: SQL query appropriate for the PostgreSQL version
    """
    compatibility = connector.version_info
    
    if query_type == 'standard':
        if compatibility.get('is_pg14_or_newer'):
            return """
                SELECT query, calls, total_exec_time, mean_exec_time, rows
                FROM pg_stat_statements
                WHERE calls > 0
                ORDER BY total_exec_time DESC
            """
        else:
            return """
                SELECT query, calls, total_time, mean_time, rows
                FROM pg_stat_statements
                WHERE calls > 0
                ORDER BY total_time DESC
            """
    
    elif query_type == 'write_activity':
        if compatibility.get('is_pg14_or_newer'):
            return """
                SELECT query, calls, total_exec_time, mean_exec_time, rows,
                       shared_blks_written, local_blks_written, temp_blks_written, wal_bytes
                FROM pg_stat_statements
                ORDER BY rows DESC, wal_bytes DESC
            """
        else:
            return """
                SELECT query, calls, total_time, mean_time, rows,
                       temp_blks_written
                FROM pg_stat_statements
                ORDER BY rows DESC
            """
    
    elif query_type == 'function_performance':
        if compatibility.get('is_pg14_or_newer'):
            return """
                SELECT query, calls, total_exec_time, mean_exec_time
                FROM pg_stat_statements
                ORDER BY total_exec_time DESC
            """
        else:
            return """
                SELECT f.funcid::regproc AS function_name,
                       s.calls, s.total_time, s.self_time
                FROM pg_stat_statements s
                JOIN pg_proc f ON s.funcid = f.oid
                ORDER BY s.total_time DESC
            """
    
    else:
        return get_pg_stat_statements_query(connector, 'standard')

def get_vacuum_progress_query(connector):
    """
    Get vacuum progress query based on PostgreSQL version.
    
    Args:
        connector (PostgresConnector): The active database connector instance.
    
    Returns:
        str: SQL query for vacuum progress
    """
    compatibility = connector.version_info
    
    if compatibility.get('is_pg17_or_newer'):
        return """
            SELECT n.nspname||'.'||c.relname AS table_name, v.phase, 
                   v.heap_blks_total, v.heap_blks_scanned, v.heap_blks_vacuumed, 
                   v.index_vacuum_count, v.max_dead_tuple_bytes, v.dead_tuple_bytes, 
                   v.num_dead_item_ids
            FROM pg_stat_progress_vacuum v 
            JOIN pg_class c ON v.relid = c.oid 
            JOIN pg_namespace n ON c.relnamespace = n.oid 
            WHERE v.datname = %(database)s
        """
    else:
        return """
            SELECT n.nspname||'.'||c.relname AS table_name, v.phase, 
                   v.heap_blks_total, v.heap_blks_scanned, v.heap_blks_vacuumed, 
                   v.index_vacuum_count, v.num_dead_tuples
            FROM pg_stat_progress_vacuum v 
            JOIN pg_class c ON v.relid = c.oid 
            JOIN pg_namespace n ON c.relnamespace = n.oid 
            WHERE v.datname = %(database)s
        """

def get_monitoring_metrics_query(connector):
    """
    Get monitoring metrics query based on PostgreSQL version.
    
    Args:
        connector (PostgresConnector): The active database connector instance.
    
    Returns:
        str: SQL query for monitoring metrics
    """
    compatibility = connector.version_info
    
    if compatibility.get('is_pg14_or_newer'):
        return """
            SELECT 
                datname, numbackends, xact_commit, xact_rollback,
                blks_read, blks_hit, tup_returned, tup_fetched,
                tup_inserted, tup_updated, tup_deleted, temp_files,
                temp_bytes, deadlocks, blk_read_time, blk_write_time
            FROM pg_stat_database 
            WHERE datname = %(database)s
        """
    else:
        return """
            SELECT 
                datname, numbackends, xact_commit, xact_rollback,
                blks_read, blks_hit, tup_returned, tup_fetched,
                tup_inserted, tup_updated, tup_deleted, temp_files,
                temp_bytes, deadlocks
            FROM pg_stat_database 
            WHERE datname = %(database)s
        """

def get_version_warnings(connector):
    """
    Get warnings for version-specific limitations.
    
    Args:
        connector (PostgresConnector): The active database connector instance.
    
    Returns:
        list: List of warning messages
    """
    compatibility = connector.version_info
    warnings = []
    
    if not compatibility.get('is_pg13_or_newer'):
        warnings.append(f"PostgreSQL {compatibility.get('version_string', 'Unknown')} is below the minimum supported version (13).")
    
    if compatibility.get('is_pg18_or_newer'):
        warnings.append(f"PostgreSQL {compatibility.get('version_string', 'Unknown')} is a development version. Not all features may be fully tested.")
    
    if compatibility.get('is_pg14') or (compatibility.get('is_pg14_or_newer') and not compatibility.get('is_pg15_or_newer')):
        warnings.append("PostgreSQL 14: pg_stat_statements no longer includes funcid. Function performance analysis will be limited.")
    
    return warnings

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
