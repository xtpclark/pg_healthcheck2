"""
Query library for the connection_metrics check.
"""

def get_total_connections_query():
    """Returns the query for total vs. max connections."""
    return "SELECT count(*) AS total_connections, (SELECT setting FROM pg_settings WHERE name = 'max_connections')::int AS max_connections FROM pg_stat_activity;"

def get_connection_states_query():
    """Returns the query for connections grouped by state."""
    return "SELECT state, count(*) FROM pg_stat_activity GROUP BY state ORDER BY count(*) DESC;"

def get_connections_by_user_db_query():
    """Returns the query for connections by user and database."""
    # The limit parameter is supplied by the calling check module.
    return "SELECT usename, datname, count(*) FROM pg_stat_activity GROUP BY usename, datname ORDER BY count(*) DESC LIMIT %(limit)s;"
