from plugins.postgres.utils.qrylib.security_audit_queries import (
    get_security_audit_query,
    get_ssl_stats_query
)

def get_weight():
    """Returns the importance score for this module."""
    return 3


def run_security_and_encryption_analysis(connector, settings):
    """
    Performs a security audit, checking for superuser roles, weak password
    encryption, and the status of SSL/TLS encrypted connections. Handles
    permission errors gracefully.
    """
    adoc_content = ["=== Security and Encryption Analysis", "Provides a summary of critical security configurations, including privileged users, password encryption methods, and connection security.\n"]
    structured_data = {}
    
    # --- Create a summary for AI Analysis ---
    summary_data = {
        "superuser_count": 0,
        "md5_password_count": 0,
        "non_ssl_connections_count": "N/A"
    }

    # --- Superuser and Password Encryption Check ---
    try:
        adoc_content.append("==== User Roles and Password Security")
        
        # NEW: Check for permissions before running the query
        if not connector.has_select_privilege('pg_authid'):
            adoc_content.append("[NOTE]\n====\nInsufficient privileges to access `pg_authid`. This check requires superuser privileges or membership in the `pg_read_all_settings` role. Skipping user role analysis.\n====\n")
            structured_data["security_audit"] = {"status": "skipped", "reason": "Insufficient privileges for pg_authid."}
        else:
            security_audit_query = get_security_audit_query(connector)
            formatted, raw = connector.execute_query(security_audit_query, return_raw=True)
            
            if "[ERROR]" in formatted:
                adoc_content.append(formatted)
                structured_data["security_audit"] = {"status": "error", "data": raw}
            elif not raw:
                adoc_content.append("[NOTE]\n====\nNo user roles found.\n====\n")
                structured_data["security_audit"] = {"status": "success", "data": []}
            else:
                adoc_content.append("[IMPORTANT]\n====\nReview roles with `is_superuser` privileges, as they bypass all permission checks. Also, identify any users with `uses_md5_password` enabled and migrate them to the more secure `scram-sha-256`.\n====\n")
                adoc_content.append(formatted)
                structured_data["security_audit"] = {"status": "success", "data": raw}
                # Populate summary for AI
                if isinstance(raw, list):
                    summary_data["superuser_count"] = sum(1 for r in raw if r.get('is_superuser'))
                    summary_data["md5_password_count"] = sum(1 for r in raw if r.get('uses_md5_password'))

    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not perform security audit: {e}\n====\n")

    # --- SSL/TLS Connection Statistics ---
    try:
        adoc_content.append("\n==== Connection Encryption (SSL/TLS)")
        
        # NEW: Check for permissions before running the query
        if not connector.has_select_privilege('pg_stat_ssl'):
            adoc_content.append("[NOTE]\n====\nInsufficient privileges to access `pg_stat_ssl`. This check requires superuser privileges or membership in the `pg_monitor` role. Skipping SSL connection analysis.\n====\n")
            structured_data["ssl_stats"] = {"status": "skipped", "reason": "Insufficient privileges for pg_stat_ssl."}
        else:
            ssl_stats_query = get_ssl_stats_query(connector)
            formatted, raw = connector.execute_query(ssl_stats_query, return_raw=True)
            
            if "[ERROR]" in formatted:
                 adoc_content.append("[NOTE]\n====\nSSL is not enabled on the server (`ssl = off` in `postgresql.conf`), or no SSL connections have been made yet. The `pg_stat_ssl` view is unavailable or empty.\n====\n")
                 structured_data["ssl_stats"] = {"status": "not_applicable", "data": raw}
            elif not raw:
                adoc_content.append("[NOTE]\n====\nNo active connections to report SSL status for.\n====\n")
                structured_data["ssl_stats"] = {"status": "success", "data": []}
            else:
                adoc_content.append("[IMPORTANT]\n====\nThis table shows the encryption status of current connections. For production systems, all connections should be encrypted (`ssl = true`).\n====\n")
                adoc_content.append(formatted)
                structured_data["ssl_stats"] = {"status": "success", "data": raw}
                # Populate summary for AI
                if isinstance(raw, list):
                    non_ssl_count = sum(r.get('connection_count', 0) for r in raw if not r.get('ssl'))
                    summary_data["non_ssl_connections_count"] = non_ssl_count

    except Exception as e:
        adoc_content.append(f"\n[ERROR]\n====\nCould not analyze SSL/TLS statistics: {e}\n====\n")

    # Add the summary to the structured data for the AI
    structured_data["security_summary"] = {"status": "success", "data": summary_data}

    adoc_content.append("\n[TIP]\n====\n* **Principle of Least Privilege**: Grant `SUPERUSER` status only when absolutely necessary. For daily operations, create specific roles with only the required permissions.\n* **Encrypt Everything**: Enforce SSL for all connections by setting `ssl = on` in `postgresql.conf` and configuring `pg_hba.conf` to require SSL (`hostssl`).\n====\n")

    return "\n".join(adoc_content), structured_data
