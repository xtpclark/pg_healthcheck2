def run_table_object_counts(cursor, settings, execute_query, execute_pgbouncer):
    """
    Counts various types of PostgreSQL database objects (tables, functions, views,
    materialized views, schemas, indexes, sequences, foreign keys, partitions)
    to provide an overview of database structure.
    """
    content = ["=== Database Object Counts", "Provides a summary count of various database object types."]
    
    if settings['show_qry'] == 'true':
        content.append("Database object count queries:")
        content.append("[,sql]\n----")
        content.append("SELECT 'Tables' AS object_type, count(*) FROM pg_class WHERE relkind = 'r' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        content.append("SELECT 'Views' AS object_type, count(*) FROM pg_class WHERE relkind = 'v' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        content.append("SELECT 'Materialized Views' AS object_type, count(*) FROM pg_class WHERE relkind = 'm' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        content.append("SELECT 'Indexes' AS object_type, count(*) FROM pg_class WHERE relkind = 'i' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        content.append("SELECT 'Sequences' AS object_type, count(*) FROM pg_class WHERE relkind = 'S' AND relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        content.append("SELECT 'Functions/Procedures' AS object_type, count(*) FROM pg_proc WHERE pronamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'pg_%' OR nspname = 'information_schema');")
        content.append("SELECT 'Schemas' AS object_type, count(*) FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema';")
        content.append("SELECT 'Foreign Keys' AS object_type, count(*) FROM pg_constraint WHERE contype = 'f';")
        content.append("SELECT 'Partitions' AS object_type, count(*) FROM pg_class WHERE relispartition = true;")
        content.append("----")

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

    # Prepare results table header
    results_table = ['|===', '|Object Type|Count']

    for object_type, query_string in object_count_queries:
        # Execute each query individually
        # No named parameters are needed for these count queries
        result = execute_query(query_string, params=None) 
        
        # Extract count from the result string, handling errors or no results
        count = "N/A"
        if "[ERROR]" in result or "[NOTE]" in result:
            count = "Error/Not Applicable"
        else:
            # Assuming execute_query returns a table like:
            # |===
            # |count
            # |123
            # |===
            # We need to parse the count from the second line
            lines = result.strip().split('\n')
            if len(lines) > 2: # Check if there's header, separator, and data
                try:
                    count = lines[2].strip().strip('|') # Get the value from the data row
                except IndexError:
                    count = "N/A (Parse Error)"

        results_table.append(f"|{object_type}|{count}")
    
    results_table.append('|===')
    content.append("Summary of Database Objects")
    content.append('\n'.join(results_table))

    content.append("[TIP]\n====\n"
                   "Monitoring object counts provides a high-level view of database complexity and growth. "
                   "A sudden increase in certain object types (e.g., tables, functions) might indicate application changes or unexpected behavior. "
                   "High numbers of indexes or foreign keys should prompt a review of their necessity and proper indexing to avoid performance overheads.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, while object counts don't directly impact managed services like storage, "
                       "they are indicative of the application's complexity. "
                       "A large number of objects can increase metadata overhead and impact DDL operations. "
                       "Ensure that object proliferation is managed, especially for temporary or unused objects.\n"
                       "====\n")
    
    return "\n".join(content)

