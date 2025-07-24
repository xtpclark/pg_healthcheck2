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

def get_available_extensions_query(connector):
    """
    Returns a query to find installed extensions that have available updates.
    The query is version-agnostic but is placed here for consistency.
    """
    return """
        SELECT name, default_version, installed_version
        FROM pg_available_extensions
        WHERE installed_version IS NOT NULL AND default_version <> installed_version;
    """

def get_high_insert_tables_query(connector):
    """
    Returns a query to identify tables with high rates of inserts, which
    can be candidates for tuning.
    """
    return """
        SELECT
            schemaname,
            relname,
            n_ins_since_vacuum AS inserts_since_last_vacuum
        FROM pg_stat_user_tables
        WHERE n_ins_since_vacuum > 100000 -- Threshold for high inserts
        ORDER BY n_ins_since_vacuum DESC
        LIMIT %(limit)s;
    """

def get_cache_hit_ratio_query(connector):
    """
    Returns a query to calculate the cache hit ratio for indexes and tables.
    A low cache hit ratio can indicate an undersized shared_buffers.
    """
    return """
        SELECT
            'index' AS object_type,
            SUM(heap_blks_read) AS heap_read,
            SUM(heap_blks_hit)  AS heap_hit,
            (SUM(heap_blks_hit) - SUM(heap_blks_read)) / SUM(heap_blks_hit) AS ratio
        FROM pg_statio_user_indexes
        UNION ALL
        SELECT
            'table' AS object_type,
            SUM(idx_blks_read) AS idx_read,
            SUM(idx_blks_hit)  AS idx_hit,
            (SUM(idx_blks_hit) - SUM(idx_blks_read)) / SUM(idx_blks_hit) AS ratio
        FROM pg_statio_user_tables;
    """

def get_vacuum_stats_query(connector):
    """
    Returns a query to find tables that may need vacuuming.
    """
    return """
        SELECT
            schemaname,
            relname,
            n_live_tup,
            n_dead_tup,
            last_autovacuum,
            last_autoanalyze
        FROM pg_stat_user_tables
        WHERE n_dead_tup > 10000 -- Threshold for dead tuples
        ORDER BY n_dead_tup DESC
        LIMIT %(limit)s;
    """


def get_security_definer_functions_query(connector):
    """
    Returns a query to find potentially insecure SECURITY DEFINER functions.
    These functions execute with the privileges of their owner, not the calling user.
    """
    return """
        SELECT
            p.proname AS function_name,
            n.nspname AS schema_name,
            pg_get_userbyid(p.proowner) AS owner
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE p.prosecdef IS TRUE
        ORDER BY schema_name, function_name
        LIMIT %(limit)s;
    """

def get_superuser_owned_functions_query(connector):
    """
    Returns a query to find functions owned by superusers. These should be reviewed
    to ensure they don't present a security risk.
    """
    return """
        SELECT
            p.proname AS function_name,
            n.nspname AS schema_name,
            pg_get_userbyid(p.proowner) AS owner
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE (SELECT rolsuper FROM pg_authid WHERE oid = p.proowner) IS TRUE
        ORDER BY schema_name, function_name
        LIMIT %(limit)s;
    """

def get_function_volatility_query(connector):
    """
    Returns a query to find functions with a 'volatile' volatility setting,
    which can prevent query parallelization.
    """
    return """
        SELECT
            n.nspname as schema_name,
            p.proname as function_name,
            CASE p.provolatile
                WHEN 'i' THEN 'immutable'
                WHEN 's' THEN 'stable'
                WHEN 'v' THEN 'volatile'
            END as volatility
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND p.provolatile = 'v'
        ORDER BY 1, 2
        LIMIT %(limit)s;
    """

def get_transaction_wraparound_query(connector):
    """
    Returns a query to check for databases approaching transaction ID wraparound.
    The formula for calculating 'percent_towards_wraparound' is version-agnostic.
    """
    return """
        SELECT
            datname,
            age(datfrozenxid),
            current_setting('autovacuum_freeze_max_age')::float8,
            round(100 * age(datfrozenxid) / current_setting('autovacuum_freeze_max_age')::float8) as percent_towards_wraparound
        FROM pg_database
        ORDER BY age(datfrozenxid) DESC
        LIMIT %(limit)s;
    """

def get_inserted_tuples_query(connector):
    """
    Returns a query to find tables with the highest number of inserted tuples.
    This helps identify tables with high write activity.
    """
    return """
        SELECT
            schemaname,
            relname,
            n_tup_ins
        FROM pg_stat_user_tables
        WHERE n_tup_ins > 0
        ORDER BY n_tup_ins DESC
        LIMIT %(limit)s;
    """

def get_top_queries_by_io_time_query(connector):
    """
    Returns a query for top queries by I/O time, supporting multiple generations
    of pg_stat_statements column names.
    """
    if connector.has_pgstat_new_io_time:
        # PG17+ style: Sums up shared, local, and temp I/O times
        return """
            SELECT
                (COALESCE(shared_blk_read_time, 0) + COALESCE(shared_blk_write_time, 0) +
                 COALESCE(local_blk_read_time, 0) + COALESCE(local_blk_write_time, 0) +
                 COALESCE(temp_blk_read_time, 0) + COALESCE(temp_blk_write_time, 0)) as total_io_time,
                calls,
                query
            FROM pg_stat_statements
            ORDER BY total_io_time DESC LIMIT %(limit)s;
        """
    elif connector.has_pgstat_legacy_io_time:
        # PG13-16 style
        return """
            SELECT
                (blk_read_time + blk_write_time) as total_io_time,
                calls,
                query
            FROM pg_stat_statements
            ORDER BY total_io_time DESC LIMIT %(limit)s;
        """
    else:
        # Fallback for older versions or outdated extensions
        return """
            SELECT
                total_exec_time as total_exec_time_as_proxy_for_io,
                calls,
                query
            FROM pg_stat_statements
            ORDER BY total_time DESC LIMIT %(limit)s;
        """
