def run_table_object_counts(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Counts various types of PostgreSQL database objects (tables, functions, views,
    materialized views, schemas, indexes, sequences, foreign keys, partitions)
    to provide an overview of database structure.
    """
    adoc_content = ["=== Database Object Counts", "Provides a summary count of various database object types."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Database object count queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("SELECT 'Tables' AS object_type, count(*) FROM pg_class WHERE relkind = 'r' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        adoc_content.append("SELECT 'Views' AS object_type, count(*) FROM pg_class WHERE relkind = 'v' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        adoc_content.append("SELECT 'Materialized Views' AS object_type, count(*) FROM pg_class WHERE relkind = 'm' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        adoc_content.append("SELECT 'Indexes' AS object_type, count(*) FROM pg_class WHERE relkind = 'i' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        adoc_content.append("SELECT 'Sequences' AS object_type, count(*) FROM pg_class WHERE relkind = 'S' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        adoc_content.append("SELECT 'Functions/Procedures' AS object_type, count(*) FROM pg_proc WHERE pronamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        adoc_content.append("SELECT 'Schemas' AS object_type, count(*) FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema';")
        adoc_content.append("SELECT 'Foreign Keys' AS object_type, count(*) FROM pg_constraint WHERE contype = 'f';")
        adoc_content.append("SELECT 'Partitions' AS object_type, count(*) FROM pg_class WHERE relispartition = true;")
        adoc_content.append("----")

    # Define individual queries for each object type
    # This makes the module more resilient to errors in a single count query
    object_count_queries = [
        ("Tables", "SELECT count(*) FROM pg_class WHERE relkind = 'r' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');"),
        ("Views", "SELECT count(*) FROM pg_class WHERE relkind = 'v' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');"),
        ("Materialized Views", "SELECT count(*) FROM pg_class WHERE relkind = 'm' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');"),
        ("Indexes", "SELECT count(*) FROM pg_class WHERE relkind = 'i' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');"),
        ("Sequences", "SELECT count(*) FROM pg_class WHERE relkind = 'S' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');"),
        ("Functions/Procedures", "SELECT count(*) FROM pg_proc WHERE pronamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');"),
        ("Schemas", "SELECT count(*) FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema';"),
        ("Foreign Keys", "SELECT count(*) FROM pg_constraint WHERE contype = 'f';"),
        ("Partitions", "SELECT count(*) FROM pg_class WHERE relispartition = true;")
    ]

    # Prepare results for the formatted table and structured data
    formatted_results_table = ['|===', '|Object Type|Count']
    structured_counts = []

    for object_type, query_string in object_count_queries:
        # Execute each query individually, requesting raw data
        # No named parameters are needed for these count queries, so params=None
        formatted_result, raw_result = execute_query(query_string, params=None, return_raw=True) 
        
        count_value = "N/A"
        status = "error"
        details = raw_result # Store raw error details if any

        if "[ERROR]" in formatted_result:
            count_value = "Error"
            status = "error"
        elif "[NOTE]" in formatted_result: # "No results returned" etc.
            count_value = "0" # For counts, "No results" implies 0
            status = "success"
            details = [] # Empty list for no data
        else:
            # Assuming raw_result for count queries is a single value (e.g., an integer)
            if isinstance(raw_result, list) and len(raw_result) > 0 and 'count' in raw_result[0]:
                count_value = raw_result[0]['count']
                status = "success"
                details = raw_result
            elif isinstance(raw_result, (int, float)): # Direct count value from execute_query(is_check=True)
                 count_value = raw_result
                 status = "success"
                 details = raw_result
            else:
                count_value = "N/A (Parse Error)"
                status = "error"
                details = "Could not parse count from raw result."

        formatted_results_table.append(f"|{object_type}|{count_value}")
        structured_counts.append({
            "object_type": object_type,
            "count": count_value,
            "status": status,
            "details": details
        })
    
    formatted_results_table.append('|===')
    adoc_content.append("Summary of Database Objects")
    adoc_content.append('\n'.join(formatted_results_table))
    structured_data["object_counts_summary"] = {"status": "success", "data": structured_counts}


    adoc_content.append("[TIP]\n====\n"
                   "Monitoring object counts provides a high-level view of database complexity and growth. "
                   "A sudden increase in certain object types (e.g., tables, functions) might indicate application changes or unexpected behavior. "
                   "High numbers of indexes or foreign keys should prompt a review of their necessity and proper indexing to avoid performance overheads.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, while object counts don't directly impact managed services like storage, "
                       "they are indicative of the application's complexity. "
                       "A large number of objects can increase metadata overhead and impact DDL operations. "
                       "Ensure that object proliferation is managed, especially for temporary or unused objects.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

