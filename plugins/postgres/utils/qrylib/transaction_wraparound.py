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
            age(d.datfrozenxid)::bigint as oldest_xid_age,
            current_setting('autovacuum_freeze_max_age')::float8 as freeze_max_age,
            round(100 * age(d.datfrozenxid)::bigint / current_setting('autovacuum_freeze_max_age')::float8) as percent_towards_wraparound
        FROM pg_database d
        ORDER BY age(d.datfrozenxid) DESC;
    """

def get_table_wraparound_query(connector):
    """
    Returns a query to find the tables with the oldest transaction IDs,
    which are the primary contributors to wraparound risk.
    """
    return """
        SELECT
            c.oid::regclass as table_name,
            c.oid,
            greatest(age(c.relfrozenxid)::bigint, age(t.relfrozenxid)::bigint) as xid_age
        FROM pg_class c
        LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
        WHERE c.relkind IN ('r', 'm')
          AND c.relfrozenxid != 0
        ORDER BY 3 DESC
        LIMIT %(limit)s;
    """

# NEW FUNCTION
def get_dead_tuples_for_table_query(connector):
    """Returns a query to get n_dead_tup for a specific table oid."""
    return "SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid = %s;"

# NEW FUNCTION
def get_autovacuum_memory_settings_query(connector):
    """Returns a query to show the current settings for vacuum memory."""
    return """
        SELECT name, setting, unit
        FROM pg_settings
        WHERE name IN ('autovacuum_work_mem', 'maintenance_work_mem');
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
