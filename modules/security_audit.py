def run_security_audit(cursor, settings, execute_query, execute_pgbouncer):
    """
    Performs a security audit of the PostgreSQL database, checking for
    common security vulnerabilities and configurations.
    """
    content = ["=== Security Audit", "Analyzes key security configurations and user privileges to identify potential vulnerabilities."]
    
    if settings['show_qry'] == 'true':
        content.append("Security audit queries:")
        content.append("[,sql]\n----")
        content.append("SELECT rolname FROM pg_roles WHERE rolsuper = true AND rolname NOT LIKE 'pg_%';")
        content.append("SELECT rolname, rolvaliduntil FROM pg_roles WHERE rolcanlogin = true AND (rolpassword IS NULL OR rolvaliduntil IS NOT NULL) AND rolname NOT LIKE 'pg_%';")
        content.append("SELECT grantee, privilege_type FROM information_schema.usage_privileges WHERE object_schema = 'public' AND object_type = 'SCHEMA' AND privilege_type IN ('CREATE', 'USAGE') ORDER BY grantee, privilege_type;")
        content.append("SELECT name, setting, short_desc FROM pg_settings WHERE name IN ('log_connections', 'log_disconnections', 'log_statement', 'ssl', 'password_encryption', 'db_user_namespace') ORDER BY name;")
        content.append("----")

    queries = [
        (
            "Superuser Roles", 
            "SELECT rolname FROM pg_roles WHERE rolsuper = true AND rolname NOT LIKE 'pg_%';", 
            True
        ),
        (
            "Roles with Password Issues (No Password or Expiration Set)", 
            "SELECT rolname, rolvaliduntil FROM pg_roles WHERE rolcanlogin = true AND (rolpassword IS NULL OR rolvaliduntil IS NOT NULL) AND rolname NOT LIKE 'pg_%';", 
            True
        ),
        (
            "Public Schema Permissions", 
            "SELECT grantee, privilege_type FROM information_schema.usage_privileges WHERE object_schema = 'public' AND object_type = 'SCHEMA' AND privilege_type IN ('CREATE', 'USAGE') ORDER BY grantee, privilege_type;", 
            True
        ),
        (
            "Key Security Settings", 
            "SELECT name, setting, short_desc FROM pg_settings WHERE name IN ('log_connections', 'log_disconnections', 'log_statement', 'ssl', 'password_encryption', 'db_user_namespace') ORDER BY name;", 
            True
        )
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        
        # Standardized parameter passing pattern:
        # In this module, none of the queries use named placeholders like %(limit)s or %(database)s.
        # Therefore, params_for_query will always be None for these queries.
        params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        
        result = execute_query(query, params=params_for_query)
        
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\n"
                   "Review superuser roles and limit them to essential administrative accounts. "
                   "Ensure all login roles have strong passwords and consider setting password expiration. "
                   "Revoke unnecessary CREATE/USAGE privileges on the public schema to prevent unauthorized object creation. "
                   "Enable connection and statement logging for auditing purposes. "
                   "Ensure SSL is enabled for secure connections. "
                   "For Aurora, manage roles and security settings via the RDS console and monitor CloudWatch Logs for security events.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora security is heavily integrated with AWS IAM and VPC. "
                       "Superuser privileges are restricted; use the RDS master user for administrative tasks. "
                       "Password management and auditing should leverage AWS Secrets Manager and CloudWatch Logs. "
                       "Ensure network access is restricted via Security Groups.\n"
                       "====\n")
    
    return "\n".join(content)
