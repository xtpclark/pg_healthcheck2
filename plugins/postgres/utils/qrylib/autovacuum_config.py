"""
Query library for the autovacuum_config check.
"""

def get_global_autovacuum_settings_query():
    """Returns the query for global autovacuum settings."""
    return "SELECT name, setting FROM pg_settings WHERE name LIKE 'autovacuum_%' ORDER BY name;"


def get_autovacuum_overrides_query():
    """Returns the query for tables with custom autovacuum overrides."""
    return """
        SELECT
            n.nspname as schema_name,
            c.relname as table_name,
            array_to_string(c.reloptions, ', ') as custom_settings
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'm')
          AND c.reloptions IS NOT NULL
          AND array_to_string(c.reloptions, ', ') LIKE '%%autovacuum%%'
        ORDER BY n.nspname, c.relname;
    """
