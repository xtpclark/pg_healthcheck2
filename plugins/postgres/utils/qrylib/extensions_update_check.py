def get_available_extensions_query(connector):
    """
    Returns a query to find installed extensions that have available updates.
    The query is version-agnostic but is placed here for consistency.
    """
    return """
        SELECT name, default_version, installed_version
        FROM pg_available_extensions
        WHERE installed_version IS NOT NULL AND default_version <> installed_version;
    """

