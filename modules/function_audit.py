def run_function_audit(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Audits database functions for security risks and performance insights.
    """
    adoc_content = ["=== Function/Stored Proc Audit","Audits database functions for security risks and performance insights."]
    structured_data = {} # Dictionary to hold structured findings for this module

    # Import version compatibility module
    from .postgresql_version_compatibility import get_postgresql_version, get_pg_stat_statements_query, validate_postgresql_version
    
    # Get PostgreSQL version compatibility information
    compatibility = get_postgresql_version(cursor, execute_query)
    
    # Validate PostgreSQL version
    is_supported, error_msg = validate_postgresql_version(compatibility)
    if not is_supported:
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["version_error"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data

    if settings['show_qry'] == 'true':
        adoc_content.append("Function audit queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("""
-- Query for SECURITY DEFINER functions
SELECT
    p.proname AS function_name,
    n.nspname AS schema_name,
    pg_get_userbyid(p.proowner) AS owner,
    p.prosecdef AS is_security_definer,
    p.proacl AS access_privileges
FROM
    pg_proc p
JOIN
    pg_namespace n ON p.pronamespace = n.oid
WHERE
    p.prosecdef IS TRUE
ORDER BY
    schema_name, function_name
LIMIT %(limit)s;
""")
        adoc_content.append("""
-- Query for functions owned by superusers
SELECT
    p.proname AS function_name,
    n.nspname AS schema_name,
    pg_get_userbyid(p.proowner) AS owner,
    (SELECT rolsuper FROM pg_authid WHERE oid = p.proowner) AS owner_is_superuser
FROM
    pg_proc p
JOIN
    pg_namespace n ON p.pronamespace = n.oid
WHERE
    (SELECT rolsuper FROM pg_authid WHERE oid = p.proowner) IS TRUE
ORDER BY
    schema_name, function_name
LIMIT %(limit)s;
""")
        if settings['has_pgstat'] == 't':
            # Get version-specific pg_stat_statements query
            pg_stat_query = get_pg_stat_statements_query(compatibility, 'function_performance')
            adoc_content.append(f"""
-- Query for function performance (PostgreSQL {compatibility['version_string']})
{pg_stat_query}
LIMIT %(limit)s;
""")
        adoc_content.append("----")

    queries = [
        (
            "Functions with SECURITY DEFINER",
            """
SELECT
    p.proname AS function_name,
    n.nspname AS schema_name,
    pg_get_userbyid(p.proowner) AS owner,
    p.proacl AS access_privileges
FROM
    pg_proc p
JOIN
    pg_namespace n ON p.pronamespace = n.oid
WHERE
    p.prosecdef IS TRUE
ORDER BY
    schema_name, function_name
LIMIT %(limit)s;
""",
            True, # Always applicable
            "security_definer_functions"
        ),
        (
            "Functions Owned by Superusers",
            """
SELECT
    p.proname AS function_name,
    n.nspname AS schema_name,
    pg_get_userbyid(p.proowner) AS owner
FROM
    pg_proc p
JOIN
    pg_namespace n ON p.pronamespace = n.oid
WHERE
    (SELECT rolsuper FROM pg_authid WHERE oid = p.proowner) IS TRUE
ORDER BY
    schema_name, function_name
LIMIT %(limit)s;
""",
            True, # Always applicable
            "superuser_owned_functions"
        )
    ]

    # Initialize flags for security findings summary
    found_security_definer = False
    found_superuser_owned = False

    # Process security-related queries first to determine summary
    security_queries_to_process = [q for q in queries if q[3] in ["security_definer_functions", "superuser_owned_functions"]]

    for title, query, condition, data_key in security_queries_to_process:
        if not condition:
            # This path is not expected for these queries, but kept for robustness
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)

        if "[ERROR]" in formatted_result:
            # Append error immediately
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            # Append successful results
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
            
            # Check for security findings to set flags
            if data_key == "security_definer_functions" and raw_result:
                found_security_definer = True
            if data_key == "superuser_owned_functions" and raw_result:
                found_superuser_owned = True

    # NEW: Add a summary of security findings at the top of the section, after the main title/description
    security_summary_lines = []
    if found_security_definer or found_superuser_owned:
        security_summary_lines.append("[IMPORTANT]\n====\nPotential security vulnerabilities identified in functions:\n")
        if found_security_definer:
            security_summary_lines.append("* One or more `SECURITY DEFINER` functions were found. These require careful review to prevent privilege escalation.\n")
        if found_superuser_owned:
            security_summary_lines.append("* One or more functions are owned by superusers. Consider reassigning ownership to less privileged roles where appropriate.\n")
        security_summary_lines.append("====\n")
        
        # Insert the summary right after the initial section title and description
        # adoc_content[0] is "=== Function Audit", adoc_content[1] is "Audits database functions..."
        adoc_content.insert(2, "\n".join(security_summary_lines))


    # Add pg_stat_statements related queries conditionally
    if settings['has_pgstat'] == 't':
        if compatibility['is_pg14_or_newer']:
            adoc_content.append("\nTop Statements by Execution Time (pg_stat_statements, PostgreSQL 14+)")
            adoc_content.append("[NOTE]\n====\nFor PostgreSQL 14 and newer, `pg_stat_statements` tracks statistics per `queryid` (hashed statement). Direct linking to function OIDs (`funcid`) is not available in this view. The following table shows top statements by execution time, which may include function calls. Manual inspection of the `query` column is required to identify specific function calls.\n====\n")
            queries.append(
                (
                    "Top Statements by Total Execution Time (pg_stat_statements)",
                    """
SELECT
    query,
    calls,
    total_exec_time,
    min_exec_time,
    max_exec_time,
    mean_exec_time
FROM
    pg_stat_statements
ORDER BY
    total_exec_time DESC
LIMIT %(limit)s;
""",
                    True, # Applicable if pg_stat_statements is enabled
                    "top_statements_by_time" # Changed key name to reflect "statements" not "functions"
                )
            )
            queries.append(
                (
                    "Top Statements by Call Count (pg_stat_statements)",
                    """
SELECT
    query,
    calls,
    total_exec_time,
    min_exec_time,
    max_exec_time,
    mean_exec_time
FROM
    pg_stat_statements
ORDER BY
    calls DESC
LIMIT %(limit)s;
""",
                    True, # Applicable if pg_stat_statements is enabled
                    "top_statements_by_calls" # Changed key name
                )
            )
        else:
            adoc_content.append("\nTop Functions by Execution Time/Calls (pg_stat_statements, PostgreSQL < 14)")
            adoc_content.append("[NOTE]\n====\nFor PostgreSQL versions before 14, `pg_stat_statements` tracks statistics per function OID (`funcid`). This provides direct function-level metrics.\n====\n")
            queries.append(
                (
                    "Top Functions by Total Execution Time (pg_stat_statements)",
                    """
SELECT
    p.proname AS function_name,
    n.nspname AS schema_name,
    s.calls,
    s.total_time,
    s.min_time,
    s.max_time,
    s.mean_time
FROM
    pg_stat_statements s
JOIN
    pg_proc p ON s.funcid = p.oid
JOIN
    pg_namespace n ON p.pronamespace = n.oid
ORDER BY
    s.total_time DESC
LIMIT %(limit)s;
""",
                    True, # Applicable if pg_stat_statements is enabled
                    "top_functions_by_time"
                )
            )
            queries.append(
                (
                    "Top Functions by Call Count (pg_stat_statements)",
                    """
SELECT
    p.proname AS function_name,
    n.nspname AS schema_name,
    s.calls,
    s.total_time,
    s.min_time,
    s.max_time,
    s.mean_time
FROM
    pg_stat_statements s
JOIN
    pg_proc p ON s.funcid = p.oid
JOIN
    pg_namespace n ON p.pronamespace = n.oid
ORDER BY
    s.calls DESC
LIMIT %(limit)s;
""",
                    True, # Applicable if pg_stat_statements is enabled
                    "top_functions_by_calls"
                )
            )
    else:
        adoc_content.append("\nTop Functions/Statements by Execution Time/Calls")
        adoc_content.append("[NOTE]\n====\n`pg_stat_statements` extension is not enabled. Enable it to get detailed function/statement performance metrics.\n====\n")
        queries.append(
            (
                "pg_stat_statements Not Enabled",
                "SELECT 'pg_stat_statements extension not enabled' AS note;",
                False, # Not applicable if pg_stat_statements is not enabled
                "pg_stat_statements_disabled"
            )
        )


    # Re-process performance related queries if pg_stat_statements is enabled
    # This loop is separate to ensure security findings are summarized first
    if settings['has_pgstat'] == 't':
        # Filter queries to only include performance related ones for this loop
        performance_queries = [q for q in queries if q[3] in ["top_statements_by_time", "top_statements_by_calls", "top_functions_by_time", "top_functions_by_calls"]]
        
        for title, query, condition, data_key in performance_queries:
            if not condition: # Should always be true here if has_pgstat is 't'
                continue
            
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
                       "Functions with `SECURITY DEFINER` can pose a security risk if not carefully managed, as they execute with the privileges of their creator, not the caller. "
                       "Review these functions to ensure their functionality is strictly necessary and their execution is limited to trusted users. "
                       "Functions owned by superusers should also be scrutinized; consider reassigning ownership to less privileged roles where possible. "
                       "Monitoring function execution time and call counts (via `pg_stat_statements`) is crucial for identifying performance bottlenecks within your application's database logic.\n"
                       "====\n")
    
    # Add a note about dynamic SQL as a future/manual audit point
    adoc_content.append("[NOTE]\n====\n"
                       "Functions employing dynamic SQL (e.g., using `EXECUTE` statements) should be manually audited for potential SQL injection vulnerabilities. "
                       "Ensure all external inputs used in dynamic queries are properly sanitized using `FORMAT()` or `quote_ident()`/`quote_literal()` to prevent malicious code execution.\n"
                       "====\n")

    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
