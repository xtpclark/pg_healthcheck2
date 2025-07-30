def get_security_audit_query(connector):
    """
    Returns a query to audit user roles and password encryption methods.
    Checks for superuser status and MD5 password usage.
    """
    # In PostgreSQL 10+, the column is `rolpassword`. Before that, it was `passwd`.
    # For simplicity across modern versions, we can check if the string starts with 'md5'.
    password_check_column = "rolpassword"
    
    return f"""
        SELECT
            rolname AS user_name,
            rolsuper AS is_superuser,
            rolcreaterole AS can_create_roles,
            rolcreatedb AS can_create_db,
            {password_check_column} ~ 'md5' AS uses_md5_password
        FROM pg_authid
        ORDER BY rolsuper DESC, rolname;
    """

def get_ssl_stats_query(connector):
    """
    Returns a query to get statistics on SSL/TLS encrypted connections.
    """
    return """
        SELECT
            ssl,
            count(*) as connection_count
        FROM pg_stat_ssl
        JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid
        GROUP BY ssl;
    """
