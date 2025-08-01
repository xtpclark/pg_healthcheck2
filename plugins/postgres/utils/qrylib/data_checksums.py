"""
Query library for the data_checksums check.
"""

def get_data_checksums_query():
    """
    Returns a query to check the status of the `data_checksums` setting.
    This query is version-agnostic.
    """
    return "SELECT name, setting FROM pg_settings WHERE name = 'data_checksums';"
