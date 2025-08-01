"""
Query library for the transaction_wraparound check.
"""
def get_database_wraparound_query(connector):
    """
    Returns a query to check each database's oldest transaction ID against
    the wraparound limit.
    """
    return """
        SELECT
            d.datname,
            age(d.datfrozenxid) as oldest_xid_age,
            current_setting('autovacuum_freeze_max_age')::float8 as freeze_max_age,
            round(100 * age(d.datfrozenxid) / current_setting('autovacuum_freeze_max_age')::float8) as percent_towards_wraparound
        FROM pg_database d
        ORDER BY age(d.datfrozenxid) DESC;
    """

def get_table_wraparound_query(connector):
    """
    Returns a query to find the tables with the oldest transaction IDs,
    which are the primary contributors to wraparound risk.
    """
    # The limit parameter is supplied by the calling check module.
    return """
        SELECT
            c.oid::regclass as table_name,
            greatest(age(c.relfrozenxid), age(t.relfrozenxid)) as xid_age,
            round(100 * greatest(age(c.relfrozenxid), age(t.relfrozenxid)) / current_setting('autovacuum_freeze_max_age')::float8) as percent_towards_wraparound
        FROM pg_class c
        LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
        WHERE c.relkind IN ('r', 'm')
          AND c.relfrozenxid != 0
        ORDER BY 2 DESC
        LIMIT %(limit)s;
    """

def get_vacuum_age_settings_query(connector):
    """
    Returns a query to show the current settings for vacuum age limits.
    """
    return """
        SELECT name, setting, unit
        FROM pg_settings
        WHERE name ~ 'autovacuum' AND name ~'_age$'
        ORDER BY 1 ASC;
    """
