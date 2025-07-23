#!/usr/bin/env python3
"""
PostgreSQL Version Compatibility Module

This module provides version-aware functionality to ensure compatibility
with PostgreSQL 13+ and future PostgreSQL 18.
"""

def get_postgresql_version(cursor, execute_query):
    """
    Get PostgreSQL version information.
    
    Returns:
        dict: Version information including version_num, version_string, and compatibility flags
    """
    try:
        # Use `current_setting` for a cleaner output and strip whitespace
        version_query = "SELECT current_setting('server_version_num');"
        _, raw_version_num = execute_query(version_query, is_check=True, return_raw=True)
        version_num = int(raw_version_num.strip())
        
        # Do the same for the version string
        version_string_query = "SELECT current_setting('server_version');"
        _, raw_version_string = execute_query(version_string_query, is_check=True, return_raw=True)
        version_string = raw_version_string.strip()
        
        # Calculate major version
        major_version = version_num // 10000
        
        # Determine compatibility flags
        compatibility = {
            'version_num': version_num,
            'version_string': version_string,
            'major_version': major_version,
            'is_pg13_or_newer': major_version >= 13,
            'is_pg14_or_newer': major_version >= 14,
            'is_pg15_or_newer': major_version >= 15,
            'is_pg16_or_newer': major_version >= 16,
            'is_pg17_or_newer': major_version >= 17,
            'is_pg18_or_newer': major_version >= 18,
            'is_pg13': major_version == 13,
            'is_pg14': major_version == 14,
            'is_pg15': major_version == 15,
            'is_pg16': major_version == 16,
            'is_pg17': major_version == 17,
            'is_pg18': major_version >= 18
        }
        
        return compatibility
        
    except Exception as e:
        # Fallback to parsing the version string if server_version_num is unavailable
        try:
            version_string_query = "SELECT current_setting('server_version');"
            _, raw_version_string = execute_query(version_string_query, is_check=True, return_raw=True)
            version_string = raw_version_string.strip()
            
            # Parse version string to get major version
            major_version = int(version_string.split('.')[0])
            version_num = major_version * 10000
            
            compatibility = {
                'version_num': version_num,
                'version_string': version_string,
                'major_version': major_version,
                'is_pg13_or_newer': major_version >= 13,
                'is_pg14_or_newer': major_version >= 14,
                'is_pg15_or_newer': major_version >= 15,
                'is_pg16_or_newer': major_version >= 16,
                'is_pg17_or_newer': major_version >= 17,
                'is_pg18_or_newer': major_version >= 18,
                'is_pg13': major_version == 13,
                'is_pg14': major_version == 14,
                'is_pg15': major_version == 15,
                'is_pg16': major_version == 16,
                'is_pg17': major_version == 17,
                'is_pg18': major_version >= 18
            }
            
            return compatibility
            
        except Exception:
            # Final fallback if all methods fail
            return {
                'version_num': 0,
                'version_string': 'unknown',
                'major_version': 0,
                'is_pg13_or_newer': False,
                'is_pg14_or_newer': False,
                'is_pg15_or_newer': False,
                'is_pg16_or_newer': False,
                'is_pg17_or_newer': False,
                'is_pg18_or_newer': False,
                'is_pg13': False,
                'is_pg14': False,
                'is_pg15': False,
                'is_pg16': False,
                'is_pg17': False,
                'is_pg18': False
            }

def get_pg_stat_statements_query(compatibility, query_type='standard'):
    """
    Get pg_stat_statements query based on PostgreSQL version.
    
    Args:
        compatibility (dict): Version compatibility information
        query_type (str): Type of query ('standard', 'write_activity', 'function_performance')
    
    Returns:
        str: SQL query appropriate for the PostgreSQL version
    """
    
    if query_type == 'standard':
        if compatibility['is_pg14_or_newer']:
            # PostgreSQL 14+ uses total_exec_time instead of total_time
            return """
                SELECT query, calls, total_exec_time, mean_exec_time, rows
                FROM pg_stat_statements
                WHERE calls > 0
                ORDER BY total_exec_time DESC
            """
        else:
            # PostgreSQL 13 and older use total_time
            return """
                SELECT query, calls, total_time, mean_time, rows
                FROM pg_stat_statements
                WHERE calls > 0
                ORDER BY total_time DESC
            """
    
    elif query_type == 'write_activity':
        if compatibility['is_pg14_or_newer']:
            # PostgreSQL 14+ has wal_bytes, shared_blks_written, etc.
            return """
                SELECT query, calls, total_exec_time, mean_exec_time, rows,
                       shared_blks_written, local_blks_written, temp_blks_written, wal_bytes
                FROM pg_stat_statements
                ORDER BY rows DESC, wal_bytes DESC
            """
        else:
            # PostgreSQL 13 and older - some columns may not be available
            return """
                SELECT query, calls, total_time, mean_time, rows,
                       temp_blks_written
                FROM pg_stat_statements
                ORDER BY rows DESC
            """
    
    elif query_type == 'function_performance':
        if compatibility['is_pg14_or_newer']:
            # PostgreSQL 14+ doesn't have funcid in pg_stat_statements
            return """
                SELECT query, calls, total_exec_time, mean_exec_time
                FROM pg_stat_statements
                ORDER BY total_exec_time DESC
            """
        else:
            # PostgreSQL 13 and older have funcid
            return """
                SELECT f.funcid::regproc AS function_name,
                       s.calls, s.total_time, s.self_time
                FROM pg_stat_statements s
                JOIN pg_proc f ON s.funcid = f.oid
                ORDER BY s.total_time DESC
            """
    
    else:
        # Default to standard query
        return get_pg_stat_statements_query(compatibility, 'standard')

def get_vacuum_progress_query(compatibility):
    """
    Get vacuum progress query based on PostgreSQL version.
    
    Args:
        compatibility (dict): Version compatibility information
    
    Returns:
        str: SQL query for vacuum progress
    """
    
    if compatibility['is_pg17_or_newer']:
        # PostgreSQL 17+ has new columns for dead tuple info
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
        # PostgreSQL 16 and older
        return """
            SELECT n.nspname||'.'||c.relname AS table_name, v.phase, 
                   v.heap_blks_total, v.heap_blks_scanned, v.heap_blks_vacuumed, 
                   v.index_vacuum_count, v.num_dead_tuples
            FROM pg_stat_progress_vacuum v 
            JOIN pg_class c ON v.relid = c.oid 
            JOIN pg_namespace n ON c.relnamespace = n.oid 
            WHERE v.datname = %(database)s
        """

def get_cache_analysis_query(compatibility):
    """
    Get cache analysis query based on PostgreSQL version.
    
    Args:
        compatibility (dict): Version compatibility information
    
    Returns:
        str: SQL query for cache analysis
    """
    
    if compatibility['is_pg14_or_newer']:
        # PostgreSQL 14+ uses total_exec_time
        return """
            SELECT 
                schemaname, tablename, attname, n_distinct, 
                correlation, most_common_vals, most_common_freqs
            FROM pg_stats 
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n_distinct DESC
        """
    else:
        # PostgreSQL 13 and older
        return """
            SELECT 
                schemaname, tablename, attname, n_distinct, 
                correlation, most_common_vals, most_common_freqs
            FROM pg_stats 
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n_distinct DESC
        """

def get_monitoring_metrics_query(compatibility):
    """
    Get monitoring metrics query based on PostgreSQL version.
    
    Args:
        compatibility (dict): Version compatibility information
    
    Returns:
        str: SQL query for monitoring metrics
    """
    
    if compatibility['is_pg14_or_newer']:
        # PostgreSQL 14+ has additional metrics
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
        # PostgreSQL 13 and older - some columns may not be available
        return """
            SELECT 
                datname, numbackends, xact_commit, xact_rollback,
                blks_read, blks_hit, tup_returned, tup_fetched,
                tup_inserted, tup_updated, tup_deleted, temp_files,
                temp_bytes, deadlocks
            FROM pg_stat_database 
            WHERE datname = %(database)s
        """

def get_version_specific_columns(compatibility, feature):
    """
    Get version-specific column names for various features.
    
    Args:
        compatibility (dict): Version compatibility information
        feature (str): Feature name ('pg_stat_statements', 'vacuum_progress', etc.)
    
    Returns:
        dict: Column mappings for the feature
    """
    
    if feature == 'pg_stat_statements':
        if compatibility['is_pg14_or_newer']:
            return {
                'execution_time': 'total_exec_time',
                'mean_time': 'mean_exec_time',
                'has_wal_bytes': True,
                'has_shared_blks_written': True,
                'has_local_blks_written': True,
                'has_funcid': False
            }
        else:
            return {
                'execution_time': 'total_time',
                'mean_time': 'mean_time',
                'has_wal_bytes': False,
                'has_shared_blks_written': False,
                'has_local_blks_written': False,
                'has_funcid': True
            }
    
    elif feature == 'vacuum_progress':
        if compatibility['is_pg17_or_newer']:
            return {
                'has_dead_tuple_bytes': True,
                'has_max_dead_tuple_bytes': True,
                'has_num_dead_item_ids': True
            }
        else:
            return {
                'has_dead_tuple_bytes': False,
                'has_max_dead_tuple_bytes': False,
                'has_num_dead_item_ids': False
            }
    
    else:
        return {}

def validate_postgresql_version(compatibility):
    """
    Validate that PostgreSQL version is supported (13+).
    
    Args:
        compatibility (dict): Version compatibility information
    
    Returns:
        tuple: (is_supported, error_message)
    """
    
    if not compatibility['is_pg13_or_newer']:
        return False, f"PostgreSQL version {compatibility['version_string']} is not supported. Minimum required version is PostgreSQL 13."
    
    return True, None

def get_version_warnings(compatibility):
    """
    Get warnings for version-specific limitations.
    
    Args:
        compatibility (dict): Version compatibility information
    
    Returns:
        list: List of warning messages
    """
    
    warnings = []
    
    if not compatibility['is_pg13_or_newer']:
        warnings.append(f"PostgreSQL {compatibility['version_string']} is below the minimum supported version (13). Some features may not work correctly.")
    
    if compatibility['is_pg18_or_newer']:
        warnings.append(f"PostgreSQL {compatibility['version_string']} is a development version. Some features may not be fully tested.")
    
    if compatibility['is_pg14_or_newer'] and not compatibility['is_pg15_or_newer']:
        warnings.append("PostgreSQL 14: pg_stat_statements no longer includes funcid column. Function performance analysis will be limited.")
    
    return warnings 
