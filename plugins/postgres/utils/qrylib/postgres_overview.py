"""
Queries for the postgres_overview check module.
"""

def get_version_query(connector):
    """Returns the query to get the database version string."""
    return "SELECT version();"

def get_database_size_query(connector):
    """Returns the query to get the current database's size."""
    return "SELECT current_database() AS database, pg_size_pretty(pg_database_size(current_database())) AS size;"

def get_uptime_query(connector):
    """Returns the query to get the database uptime."""
    return "SELECT date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;"

def get_key_config_query(connector):
    """Returns the query for key memory and connection configuration settings."""
    return """
        SELECT name, setting, unit 
        FROM pg_settings 
        WHERE name IN ('max_connections', 'work_mem', 'shared_buffers', 'effective_cache_size') 
        ORDER BY name;
    """
