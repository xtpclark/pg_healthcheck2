def run_extensions_update_check(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Checks for installed extensions that have newer versions available in the database.
    Flags extensions that can be updated and provides recommendations.
    """
    adoc_content = ["=== Extensions Requiring Update\n", "Checks for installed extensions that have newer versions available in the database.\n"]
    structured_data = {}

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