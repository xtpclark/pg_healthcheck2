def run_security_and_encryption_analysis(connector, settings):
    """
    Performs a security audit, checking for superuser roles, weak password
    encryption, and the status of SSL/TLS encrypted connections.
    """
    adoc_content = ["=== Security and Encryption Analysis", "Provides a summary of critical security configurations, including privileged users, password encryption methods, and connection security.\n"]
    structured_data = {}

    # --- Superuser and Password Encryption Check ---
    try:
        adoc_content.append("==== User Roles and Password Security")
        security_audit_query = """
            SELECT
                rolname AS user_name,
                rolsuper AS is_superuser,
                rolcreaterole AS can_create_roles,
                rolcreatedb AS can_create_db,
                passwd ~ 'md5' AS uses_md5_password
            FROM pg_authid
            ORDER BY rolsuper DESC, rolname;
        """
        formatted, raw = connector.execute_query(security_audit_query, return_raw=True)
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo user roles found.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nReview roles with `is_superuser` privileges, as they bypass all permission checks. Also, identify any users with `uses_md5_password` enabled and migrate them to the more secure `scram-sha-256`.\n====\n")
            adoc_content.append(formatted)
        structured_data["security_audit"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not perform security audit: {e}\n====\n")

    # --- SSL/TLS Connection Statistics ---
    try:
        adoc_content.append("\n==== Connection Encryption (SSL/TLS)")
        ssl_stats_query = """
            SELECT
                ssl,
                count(*) as connection_count
            FROM pg_stat_ssl
            JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid
            GROUP BY ssl;
        """
        formatted, raw = connector.execute_query(ssl_stats_query, return_raw=True)
        if "[ERROR]" in formatted:
            # Check if the error is because ssl is not enabled, which is a common case
            if "pg_stat_ssl" in str(raw.get("error", "")):
                 adoc_content.append("[NOTE]\n====\nSSL is not enabled on the server (`ssl = off` in `postgresql.conf`), or no connections have been made yet. The `pg_stat_ssl` view is unavailable.\n====\n")
            else:
                adoc_content.append(formatted)
        elif not raw:
            adoc_content.append("[NOTE]\n====\nNo active connections to report SSL status for.\n====\n")
        else:
            adoc_content.append("[IMPORTANT]\n====\nThis table shows the encryption status of current connections. For production systems, all connections should be encrypted (`ssl = true`).\n====\n")
            adoc_content.append(formatted)
        structured_data["ssl_stats"] = {"status": "success", "data": raw}
    except Exception as e:
        adoc_content.append(f"[ERROR]\n====\nCould not analyze SSL/TLS statistics: {e}\n====\n")

    adoc_content.append("\n[TIP]\n====\n* **Principle of Least Privilege**: Grant `SUPERUSER` status only when absolutely necessary. For daily operations, create specific roles with only the required permissions.\n* **Encrypt Everything**: Enforce SSL for all connections by setting `ssl = on` in `postgresql.conf` and configuring `pg_hba.conf` to require SSL (`hostssl`).\n====\n")

    return "\n".join(adoc_content), structured_data
