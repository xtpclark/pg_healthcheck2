def run_invalid_idx(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies invalid indexes in the PostgreSQL database.
    """
    adoc_content = ["=== Invalid Indexes\n", "Identifies invalid indexes in the PostgreSQL database. Invalid indexes can cause query planner errors and should be rebuilt or dropped.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module

    # Import version compatibility module
    from .postgresql_version_compatibility import get_postgresql_version, validate_postgresql_version
    
    # Get PostgreSQL version compatibility information
    compatibility = get_postgresql_version(cursor, execute_query)
    
    # Validate PostgreSQL version
    is_supported, error_msg = validate_postgresql_version(compatibility)
    if not is_supported:
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["version_error"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data

    if settings['show_qry'] == 'true':
        adoc_content.append("Invalid indexes query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT n.nspname AS schemaname, c.relname AS tablename, i.relname AS indexname, pg_get_indexdef(i.oid) AS indexdef")
        adoc_content.append("FROM pg_index x")
        adoc_content.append("JOIN pg_class c ON c.oid = x.indrelid")
        adoc_content.append("JOIN pg_class i ON i.oid = x.indexrelid")
        adoc_content.append("JOIN pg_namespace n ON n.oid = c.relnamespace")
        adoc_content.append("WHERE x.indisvalid = false")
        adoc_content.append("ORDER BY n.nspname, c.relname, i.relname")
        adoc_content.append("LIMIT %(limit)s;")
        adoc_content.append("----")

    query = '''
SELECT n.nspname AS schemaname, c.relname AS tablename, i.relname AS indexname, pg_get_indexdef(i.oid) AS indexdef
FROM pg_index x
JOIN pg_class c ON c.oid = x.indrelid
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE x.indisvalid = false
ORDER BY n.nspname, c.relname, i.relname
LIMIT %(limit)s;
'''
    params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None

    formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)

    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Invalid Indexes\n{formatted_result}")
        structured_data["invalid_indexes"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("Invalid Indexes")
        adoc_content.append(formatted_result)
        structured_data["invalid_indexes"] = {"status": "success", "data": raw_result}

    adoc_content.append("\n[IMPORTANT]\n====\n")
    adoc_content.append("**⚠️ Invalid indexes can cause query planner errors and may prevent some queries from using indexes efficiently.**\n\n")
    adoc_content.append("- Invalid indexes are usually the result of a failed CREATE INDEX CONCURRENTLY or a crash during index build.\n")
    adoc_content.append("- PostgreSQL will not use invalid indexes for query planning.\n")
    adoc_content.append("- Invalid indexes should be rebuilt or dropped as soon as possible.\n\n")
    adoc_content.append("**Recommended Actions:**\n")
    adoc_content.append("1. Rebuild the index using: `REINDEX INDEX <indexname>;`\n")
    adoc_content.append("2. Or drop and recreate the index if it is no longer needed.\n")
    adoc_content.append("3. Investigate the cause of index invalidation (e.g., failed CREATE INDEX CONCURRENTLY, crash, or disk issue).\n")
    adoc_content.append("====\n")

    adoc_content.append("[TIP]\n====\n")
    adoc_content.append("**Best Practice:** Regularly check for invalid indexes, especially after failed index operations or database crashes.\n")
    adoc_content.append("====\n")

    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data 