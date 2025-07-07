def run_users(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes database user roles and permissions to ensure secure access control.
    """
    adoc_content = ["=== User Analysis", "Analyzes database user roles and permissions to ensure secure access control."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("User analysis queries:")
        adoc_content.append("[,sql]\n----")
        # Explicitly escape % for display if needed, but for actual query, psycopg2 handles it.
        # The issue is usually when mixing %s and %(name)s.
        adoc_content.append("SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname NOT LIKE 'pg_%%';")
        adoc_content.append("SELECT r.rolname, n.nspname||'.'||c.relname AS object, p.privilege_type FROM pg_roles r JOIN pg_class c ON c.relowner = r.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN information_schema.table_privileges p ON p.grantee = r.rolname WHERE c.relkind = 'r' AND r.rolname NOT LIKE 'pg_%%' ORDER BY r.rolname, n.nspname, c.relname LIMIT %(limit)s;")
        adoc_content.append("----")

    queries = [
        (
            "User Roles", 
            "SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname NOT LIKE 'pg_%%';", # Escaped %
            True,
            "user_roles" # Data key
        ),
        (
            "User Permissions on Tables", 
            "SELECT r.rolname, n.nspname||'.'||c.relname AS object, p.privilege_type FROM pg_roles r JOIN pg_class c ON c.relowner = r.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN information_schema.table_privileges p ON p.grantee = r.rolname WHERE c.relkind = 'r' AND r.rolname NOT LIKE 'pg_%%' ORDER BY r.rolname, n.nspname, c.relname LIMIT %(limit)s;", # Escaped %
            True,
            "user_table_permissions" # Data key
        )
    ]

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        # Check if the query contains any named placeholders before creating params dict
        if '%(' in query:
            params_for_query = {'limit': settings['row_limit']}
            # Add other named parameters if they exist in the query
            # For 'User Roles', this will be None
            # For 'User Permissions on Tables', this will be {'limit': ...}
        else:
            params_for_query = None # No parameters needed if no named placeholders

        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\nEnsure users have minimal required permissions (principle of least privilege). Avoid granting superuser or create role privileges unless necessary. For Aurora, manage roles via the RDS console and monitor user activity via CloudWatch Logs.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nAWS RDS Aurora restricts superuser privileges. Use the RDS console to manage roles and permissions.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
