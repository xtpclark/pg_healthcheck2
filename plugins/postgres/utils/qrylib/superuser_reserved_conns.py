def get_superuser_reserved(connector):
    """Returns superuser reserved connections"""
    return """
        SELECT setting::int FROM pg_settings WHERE name = 'superuser_reserved_connections';
    """
