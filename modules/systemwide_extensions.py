def run_systemwide_extensions(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Lists all installed PostgreSQL extensions in the current database,
    providing an overview of available and used functionalities.
    """
    adoc_content = ["=== System-Wide Extensions", "Lists all installed PostgreSQL extensions."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Extensions query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT extname, extversion, extowner::regrole, extnamespace::regnamespace, extrelocatable, extconfig FROM pg_extension ORDER BY extname;")
        adoc_content.append("----")

    query = "SELECT extname, extversion, extowner::regrole, extnamespace::regnamespace, extrelocatable, extconfig FROM pg_extension ORDER BY extname;"
    
    # No parameters needed for this query
    params_for_query = None 
    
    formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"System-Wide Extensions\n{formatted_result}")
        structured_data["extensions_list"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("System-Wide Extensions")
        adoc_content.append(formatted_result)
        structured_data["extensions_list"] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\n"
                   "Review installed extensions to understand additional functionalities available in your database. "
                   "Ensure all necessary extensions are installed and unnecessary ones are removed to minimize attack surface. "
                   "Be aware of extensions that might consume significant resources or introduce compatibility issues during upgrades.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora supports a subset of PostgreSQL extensions. "
                       "Always check the AWS documentation for supported extensions and their versions. "
                       "Some extensions might have specific installation or configuration steps in Aurora.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

