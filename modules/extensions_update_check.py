def run_extensions_update_check(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Checks for installed extensions that have newer versions available in the database.
    Flags extensions that can be updated and provides recommendations.
    """
    adoc_content = ["=== Extensions Requiring Update\n", "Checks for installed extensions that have newer versions available in the database.\n"]
    structured_data = {}

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

    # Check if pg_available_extension_versions is available (PostgreSQL 9.1+)
    if compatibility['version_num'] < 90100:
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append("Extension version checking requires PostgreSQL 9.1 or newer. ")
        adoc_content.append(f"Current version: {compatibility['version_string']}\n")
        adoc_content.append("====\n")
        structured_data["extensions_update_check"] = {"status": "not_supported", "reason": "PostgreSQL version too old"}
        return "\n".join(adoc_content), structured_data

    if settings['show_qry'] == 'true':
        adoc_content.append("Extensions update check query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT e.extname, e.extversion AS installed_version, a.version AS available_version\n"
            "FROM pg_extension e\n"
            "JOIN pg_available_extension_versions a ON e.extname = a.name\n"
            "WHERE a.version = (SELECT max(version) FROM pg_available_extension_versions WHERE name = e.extname)\n"
            "  AND e.extversion < a.version\n"
            "ORDER BY e.extname;")
        adoc_content.append("----")

    query = '''
SELECT e.extname, e.extversion AS installed_version, a.version AS available_version
FROM pg_extension e
JOIN pg_available_extension_versions a ON e.extname = a.name
WHERE a.version = (SELECT max(version) FROM pg_available_extension_versions WHERE name = e.extname)
  AND e.extversion < a.version
ORDER BY e.extname;
'''
    params_for_query = None

    formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)

    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Extensions Update Check\n{formatted_result}")
        structured_data["extensions_update_check"] = {"status": "error", "details": raw_result}
    else:
        if not raw_result:
            adoc_content.append("All installed extensions are up to date.")
            structured_data["extensions_update_check"] = {"status": "success", "data": []}
        else:
            adoc_content.append("The following extensions have newer versions available:")
            adoc_content.append(formatted_result)
            adoc_content.append("[IMPORTANT]\n====\nTo update an extension, run:\n\nALTER EXTENSION <extension_name> UPDATE;\n\nAlways test extension updates in a staging environment before applying to production.\n====\n")
            structured_data["extensions_update_check"] = {"status": "success", "data": raw_result}

    adoc_content.append("[TIP]\n====\n")
    adoc_content.append("Regularly updating extensions ensures you benefit from bug fixes, performance improvements, and new features. ")
    adoc_content.append("Check extension release notes for compatibility and breaking changes before updating.\n")
    adoc_content.append("====\n")

    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data 