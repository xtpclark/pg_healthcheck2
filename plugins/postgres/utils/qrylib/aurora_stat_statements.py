"""
Version-aware queries for the Aurora-specific aurora_stat_statements extension.
"""

def _is_supported_version(connector):
    """
    Checks if the Aurora PostgreSQL version supports aurora_stat_statements().
    - 15.4 and higher 15 versions
    - 14.9 and higher 14 versions
    - 16.0 and higher 16 versions
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
    """
    Checks if the Aurora PostgreSQL version supports memory columns.
    - 16.3 and higher versions
    - 15.7 and higher versions
    - 14.12 and higher versions
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
    """
    Returns the query for the aurora_stat_statements_summary() function,
    or None if the version is not supported.
    """
    if not _is_supported_version(connector):
        return None
    return "SELECT * FROM aurora_stat_statements_summary();"

def get_aurora_stat_statements_details_query(connector):
    """
    Returns the query for the aurora_stat_statements() function,
    dynamically adding memory columns for supported versions.
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
