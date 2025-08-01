"""
Query library for the cpu_intensive_queries check.
"""

def get_aurora_replica_status_query():
    """Returns the query for Aurora-specific replica status."""
    return "SELECT replica_lag, replica_lag_size FROM aurora_replica_status();"

def get_active_connections_query():
    """Returns the query for active connections, excluding autovacuum."""
    # The autovacuum_pattern parameter is supplied by the calling check.
    return "SELECT state, count(*) FROM pg_stat_activity WHERE state = 'active' AND query NOT LIKE %(autovacuum_pattern)s GROUP BY state;"
