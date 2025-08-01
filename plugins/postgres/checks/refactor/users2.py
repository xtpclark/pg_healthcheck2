def run_users(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== User Analysis", "Analyzes database user roles and permissions to ensure secure access control."]
    
    if settings['show_qry'] == 'true':
        content.append("User analysis queries:")
        content.append("[,sql]\n----")
        content.append("SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname NOT LIKE 'pg_%';")
        content.append("SELECT r.rolname, n.nspname||'.'||c.relname AS object, p.privilege_type FROM pg_roles r JOIN pg_class c ON c.relowner = r.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN information_schema.table_privileges p ON p.grantee = r.rolname WHERE c.relkind = 'r' AND r.rolname NOT LIKE 'pg_%' ORDER BY r.rolname, n.nspname, c.relname LIMIT %(limit)s;")
        content.append("----")

    queries = [
        ("User Roles", "SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname NOT LIKE 'pg_%';", True),
        ("User Permissions on Tables", "SELECT r.rolname, n.nspname||'.'||c.relname AS object, p.privilege_type FROM pg_roles r JOIN pg_class c ON c.relowner = r.oid JOIN pg_namespace n ON c.relnamespace = n.oid JOIN information_schema.table_privileges p ON p.grantee = r.rolname WHERE c.relkind = 'r' AND r.rolname NOT LIKE 'pg_%' ORDER BY r.rolname, n.nspname, c.relname LIMIT %(limit)s;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        params = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        result = execute_query(query, params=params)
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\nEnsure users have minimal required permissions (principle of least privilege). Avoid granting superuser or create role privileges unless necessary. For Aurora, manage roles via the RDS console and monitor user activity via CloudWatch Logs.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora restricts superuser privileges. Use the RDS console to manage roles and permissions.\n====")
    
    return "\n".join(content)
