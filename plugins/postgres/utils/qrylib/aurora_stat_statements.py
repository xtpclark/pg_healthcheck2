"""Version-aware queries for the Aurora-specific pg_stat_statements extension.

This module provides functions to generate SQL queries for the
`aurora_stat_statements()` and `aurora_stat_statements_summary()` functions.
It dynamically constructs queries based on the connected database version
to ensure compatibility and leverage new features like memory tracking
columns when available.
"""

def _is_supported_version(connector):
    """Checks if the Aurora PostgreSQL version supports aurora_stat_statements().

    The `aurora_stat_statements()` function is supported on specific minor
    versions of Aurora PostgreSQL. This function checks against those known
    versions. Supported versions are: 14.9+, 15.4+, and all 16.x+ versions.

    Args:
        connector (object): The database connector, which holds the version
            information for the connected instance.

    Returns:
        bool: True if the database version is supported, False otherwise.
    """

    version_num = connector.version_info.get('version_num', 0)
    major_version = connector.version_info.get('major_version', 0)

    if major_version == 15 and version_num >= 150004:
        return True
    if major_version == 14 and version_num >= 140009:
        return True
    if major_version >= 16:
        return True
        
    return False

def _has_memory_columns(connector):
    """Checks if the version supports memory columns in aurora_stat_statements().

    Memory usage columns were added in later minor releases. This function
    checks if the connected version is one of them. Supported versions are:
    14.12+, 15.7+, and 16.3+.

    Args:
        connector (object): The database connector, which holds the version
            information.

    Returns:
        bool: True if memory columns are supported, False otherwise.
    """
    version_num = connector.version_info.get('version_num', 0)
    major_version = connector.version_info.get('major_version', 0)

    if major_version == 16 and version_num >= 160003:
        return True
    if major_version == 15 and version_num >= 150007:
        return True
    if major_version == 14 and version_num >= 140012:
        return True
        
    return False

def get_aurora_stat_statements_summary_query(connector):
    """Returns the query for the aurora_stat_statements_summary() function.

    This function first checks if the database version supports the feature
    before returning the query string.

    Args:
        connector (object): The database connector, used for version checking.

    Returns:
        str | None: The SQL query string if the version is supported,
        otherwise None.
    """
    if not _is_supported_version(connector):
        return None
    return "SELECT * FROM aurora_stat_statements_summary();"

def get_aurora_stat_statements_details_query(connector):
    """Returns a version-aware query for the aurora_stat_statements() function.

    This function dynamically constructs the SELECT statement, including the
    optional memory-related columns only if the connected database version
    supports them. The query is ordered by cpu_time to find the most
    expensive statements.

    Args:
        connector (object): The database connector, used for version checking.

    Returns:
        str | None: The complete, version-aware SQL query string if the
        database version is supported, otherwise None.
    """

    if not _is_supported_version(connector):
        return None
    
    # Start with the base query
    query = """
        SELECT
            instance_id,
            pid,
            datid,
            queryid,
            user_id,
            query,
            calls,
            total_time,
            cpu_time
    """
    
    # Conditionally add memory columns if supported
    if _has_memory_columns(connector):
        query += """,
            total_mem_usage,
            local_peak_mem,
            shared_peak_mem
        """
    
    # Add the rest of the query
    query += """
        FROM aurora_stat_statements()
        ORDER BY cpu_time DESC
        LIMIT %(limit)s;
    """
    
    return query
