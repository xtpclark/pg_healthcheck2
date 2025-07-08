def run_dupe_idx(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies duplicate indexes in the PostgreSQL database.
    Duplicate indexes are redundant and waste storage and write overhead.
    """
#    adoc_content = ["=== Duplicate Indexes", "Identifies duplicate indexes in the PostgreSQL database."]
    adoc_content = ["Identifies duplicate indexes in the PostgreSQL database.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Duplicate index query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("""
SELECT pg_size_pretty(sum(pg_relation_size(idx))::bigint) AS size,
       (array_agg(idx))[1] AS idx1, (array_agg(idx))[2] AS idx2
FROM (SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\\n'|| indclass::text ||E'\\n'|| indkey::text ||E'\\n'||
        coalesce(indexprs::text,'')||E'\\n' || coalesce(indpred::text,'')) AS KEY
        FROM pg_index) sub
GROUP BY KEY HAVING count(*)>1
ORDER BY sum(pg_relation_size(idx)) DESC LIMIT %(limit)s;
""")
        adoc_content.append("----")

    # Condition to check if any duplicate indexes exist
    condition_query = """
SELECT EXISTS (SELECT 1 FROM (
                SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\\n'|| indclass::text ||E'\\n'|| indkey::text ||E'\\n'||
                        coalesce(indexprs::text,'')||E'\\n'|| coalesce(indpred::text,'')) AS KEY
                        FROM pg_index) sub
                GROUP BY KEY HAVING count(*)>1);
"""
    chk_result_formatted, chk_result_raw = execute_query(condition_query, is_check=True, return_raw=True)

    # Main query for duplicate indexes
    main_query = """
SELECT pg_size_pretty(sum(pg_relation_size(idx))::bigint) AS size,
       (array_agg(idx))[1] AS idx1, (array_agg(idx))[2] AS idx2
FROM (SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\\n'|| indclass::text ||E'\\n'|| indkey::text ||E'\\n'||
        coalesce(indexprs::text,'')||E'\\n' || coalesce(indpred::text,'')) AS KEY
        FROM pg_index) sub
GROUP BY KEY HAVING count(*)>1
ORDER BY sum(pg_relation_size(idx)) DESC LIMIT %(limit)s;
"""
    params_for_query = {'limit': settings['row_limit']}

    if chk_result_raw == 'f': # Use the raw boolean result for the condition check
        adoc_content.append("[NOTE]\n====\nNo duplicate indexes were found.\nDuplicate or multiple indexes that have the same set of columns, same opclass, expression and predicate make them equivalent.\n====\n")
        structured_data["duplicate_indexes"] = {"status": "success", "data": []}
    else:
        formatted_result, raw_result = execute_query(main_query, params=params_for_query, return_raw=True)
        
        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["duplicate_indexes"] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(formatted_result)
            adoc_content.append("[CAUTION]\n====\nDuplicate indexes were found.\nDuplicate or multiple indexes that have the same set of columns, same opclass, expression and predicate make them equivalent.\n\n. Review source code to determine where they may have originated from.\n. Understand why a duplicate might exist.\n. It may have had some purpose at some point.\n. Test thoroughly before taking any action.\n. Do not automate the task of dropping one of them.\n====\n")
            structured_data["duplicate_indexes"] = {"status": "success", "data": raw_result}
    
    adoc_content.append("[TIP]\n====\n"
                   "Duplicate indexes consume unnecessary disk space and add overhead to write operations. "
                   "They do not provide additional performance benefits for queries. "
                   "Identify and remove redundant indexes to improve write performance and reduce storage costs.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "On AWS RDS Aurora, duplicate indexes increase storage consumption and `WriteIOPS`. "
                       "Removing them can lead to cost savings and improved write performance. "
                       "Always verify the uniqueness and purpose of indexes before dropping them.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

