def run_security_audit(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Performs a security audit of the PostgreSQL database, checking for
    common security vulnerabilities and configurations.
    """
    adoc_content = ["=== Security Audit", "Analyzes key security configurations and user privileges to identify potential vulnerabilities.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Security audit queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT rolname FROM pg_roles WHERE rolsuper = true AND rolname NOT LIKE 'pg_%';")
        adoc_content.append("SELECT rolname, rolvaliduntil FROM pg_roles WHERE rolcanlogin = true AND (rolpassword IS NULL OR rolvaliduntil IS NOT NULL) AND rolname NOT LIKE 'pg_%';")
        adoc_content.append("SELECT grantee, privilege_type FROM information_schema.usage_privileges WHERE object_schema = 'public' AND object_type = 'SCHEMA' AND privilege_type IN ('CREATE', 'USAGE') ORDER BY grantee, privilege_type;")
        adoc_content.append("SELECT name, setting, short_desc FROM pg_settings WHERE name IN ('log_connections', 'log_disconnections', 'log_statement', 'ssl', 'password_encryption', 'db_user_namespace') ORDER BY name;")
        adoc_content.append("----")

    queries = [
        (
            "Superuser Roles", 
            "SELECT rolname FROM pg_roles WHERE rolsuper = true AND rolname NOT LIKE 'pg_%';", 
            True,
            "superuser_roles" # Data key
        ),
        (
            "Roles with Password Issues (No Password or Expiration Set)", 
            "SELECT rolname, rolvaliduntil FROM pg_roles WHERE rolcanlogin = true AND (rolpassword IS NULL OR rolvaliduntil IS NOT NULL) AND rolname NOT LIKE 'pg_%';", 
            True,
            "roles_with_password_issues" # Data key
        ),
        (
            "Public Schema Permissions", 
            "SELECT grantee, privilege_type FROM information_schema.usage_privileges WHERE object_schema = 'public' AND object_type = 'SCHEMA' AND privilege_type IN ('CREATE', 'USAGE') ORDER BY grantee, privilege_type;", 
            True,
            "public_schema_permissions" # Data key
        ),
        (
            "Key Security Settings", 
            "SELECT name, setting, short_desc FROM pg_settings WHERE name IN ('log_connections', 'log_disconnections', 'log_statement', 'ssl', 'password_encryption', 'db_user_namespace') ORDER BY name;", 
            True,
            "key_security_settings" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\n"
                   "Review superuser roles and limit them to essential administrative accounts. "
                   "Ensure all login roles have strong passwords and consider setting password expiration. "
                   "Revoke unnecessary CREATE/USAGE privileges on the public schema to prevent unauthorized object creation. "
                   "Enable connection and statement logging for auditing purposes. "
                   "Ensure SSL is enabled for secure connections. "
                   "For Aurora, manage roles and security settings via the RDS console and monitor CloudWatch Logs for security events.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora security is heavily integrated with AWS IAM and VPC. "
                       "Superuser privileges are restricted; use the RDS master user for administrative tasks. "
                       "Password management and auditing should leverage AWS Secrets Manager and CloudWatch Logs. "
                       "Ensure network access is restricted via Security Groups.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

