# pg_healthcheck2/plugins/postgres/utils/qrylib/long_running_queries.py

"""
Query library for the long_running_queries check.
"""

def get_long_running_queries_query(connector):
    """
    Returns a version-aware query to find long-running active queries.

    The columns for wait events were added in PostgreSQL 9.6. This function includes
    them for versions 10 and newer, ensuring broader compatibility.

    Args:
        connector: The database connector object, used to access version info.

    Returns:
        A SQL query string with a parameter for the time threshold.
    """
    version_info = connector.version_info
    
    # Conditionally include wait_event columns based on PostgreSQL version.
    wait_event_column = "wait_event_type, wait_event" if version_info.get('major_version', 0) >= 10 else "'N/A' AS wait_event_type, 'N/A' AS wait_event"
    
    # The `long_running_query_seconds` parameter is passed at execution time
    # from the main check module.
    return f"""
        SELECT
            pid,
            usename,
            application_name,
            client_addr,
            state,
            {wait_event_column},
            age(clock_timestamp(), query_start) AS duration,
            query
        FROM pg_stat_activity
        WHERE state <> 'idle'
          AND (backend_type = 'client backend' OR backend_type IS NULL)
          AND age(clock_timestamp(), query_start) > (%(long_running_query_seconds)s * interval '1 second')
        ORDER BY duration DESC;
    """
