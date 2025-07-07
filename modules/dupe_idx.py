def run_dupe_idx(cursor, settings, execute_query, execute_pgbouncer):
    """
    Identifies duplicate indexes in the PostgreSQL database.
    Duplicate indexes are redundant and waste storage and write overhead.
    """
    content = ["=== Duplicate Indexes", "Identifies duplicate indexes in the PostgreSQL database."]
    
    if settings['show_qry'] == 'true':
        content.append("Duplicate index query:")
        content.append("[,sql]\n----")
        content.append("""
SELECT pg_size_pretty(sum(pg_relation_size(idx))::bigint) AS size,
       (array_agg(idx))[1] AS idx1, (array_agg(idx))[2] AS idx2
FROM (SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\\n'|| indclass::text ||E'\\n'|| indkey::text ||E'\\n'||
        coalesce(indexprs::text,'')||E'\\n' || coalesce(indpred::text,'')) AS KEY
        FROM pg_index) sub
GROUP BY KEY HAVING count(*)>1
ORDER BY sum(pg_relation_size(idx)) DESC LIMIT %(limit)s;
""")
        content.append("----")

    # Condition to check if any duplicate indexes exist
    condition_query = """
SELECT EXISTS (SELECT 1 FROM (
                SELECT indexrelid::regclass AS idx, (indrelid::text ||E'\\n'|| indclass::text ||E'\\n'|| indkey::text ||E'\\n'||
                        coalesce(indexprs::text,'')||E'\\n'|| coalesce(indpred::text,'')) AS KEY
                        FROM pg_index) sub
                GROUP BY KEY HAVING count(*)>1);
"""
    chk_result = execute_query(condition_query, is_check=True)

    if chk_result == 'f':
        content.append("[NOTE]\n====\nNo duplicate indexes were found.\nDuplicate or multiple indexes that have the same set of columns, same opclass, expression and predicate make them equivalent.\n====\n")
    else:
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
        result = execute_query(main_query, params=params_for_query)
        
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(result)
        else:
            content.append(result)
            content.append("[CAUTION]\n====\nDuplicate indexes were found.\nDuplicate or multiple indexes that have the same set of columns, same opclass, expression and predicate make them equivalent.\n\n. Review source code to determine where they may have originated from.\n. Understand why a duplicate might exist.\n. It may have had some purpose at some point.\n. Test thoroughly before taking any action.\n. Do not automate the task of dropping one of them.\n====\n")
    
    content.append("[TIP]\n====\n"
                   "Duplicate indexes consume unnecessary disk space and add overhead to write operations. "
                   "They do not provide additional performance benefits for queries. "
                   "Identify and remove redundant indexes to improve write performance and reduce storage costs.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "On AWS RDS Aurora, duplicate indexes increase storage consumption and `WriteIOPS`. "
                       "Removing them can lead to cost savings and improved write performance. "
                       "Always verify the uniqueness and purpose of indexes before dropping them.\n"
                       "====\n")
    
    return "\n".join(content)

