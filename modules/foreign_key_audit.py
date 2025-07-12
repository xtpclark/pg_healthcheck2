def run_foreign_key_audit(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Performs an audit of foreign keys, focusing on identifying potential
    write-amplification issues, especially due to missing indexes on FK columns.
    """
    adoc_content = ["=== Foreign Key Audit\nAudits foreign key constraints to identify potential write-amplification issues and ensure data integrity.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Foreign key audit queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("""
SELECT
    conname AS foreign_key_name,
    conrelid::regclass AS child_table,
    pg_get_constraintdef(c.oid) AS constraint_definition
FROM
    pg_constraint c
WHERE
    contype = 'f'
ORDER BY
    conrelid::regclass
LIMIT %(limit)s;
""")
        adoc_content.append("""
SELECT
    fk.conname AS foreign_key_name,
    n_child.nspname || '.' || fk_table.relname AS child_table,
    ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = fk.conrelid AND attnum = ANY(fk.conkey)) AS fk_col_names,
    n_parent.nspname || '.' || pk_table.relname AS parent_table,
    ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = fk.confrelid AND attnum = ANY(fk.confkey)) AS pk_col_names
FROM
    pg_constraint fk
JOIN
    pg_class fk_table ON fk_table.oid = fk.conrelid
JOIN
    pg_namespace n_child ON n_child.oid = fk_table.relnamespace
JOIN
    pg_class pk_table ON pk_table.oid = fk.confrelid
JOIN
    pg_namespace n_parent ON n_parent.oid = pk_table.relnamespace
WHERE
    fk.contype = 'f'
    AND NOT EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indrelid = fk.conrelid
        AND i.indisvalid AND i.indisready -- Ensure index is valid and ready
        AND array_length(i.indkey, 1) >= array_length(fk.conkey, 1) -- Index must have at least as many columns as FK
        AND (
            -- Check if the foreign key columns are the leading columns of the index
            -- Convert indkey to int[] for robust comparison with conkey (smallint[])
            (SELECT array_agg(x::int) FROM unnest(i.indkey[:array_length(fk.conkey, 1)]) as x) = (SELECT array_agg(x::int) FROM unnest(fk.conkey) as x)
        )
    )
ORDER BY
    child_table, foreign_key_name
LIMIT %(limit)s;
""")
        adoc_content.append("----")

    queries = [
        (
            "All Foreign Keys Defined", 
            """
SELECT
    conname AS foreign_key_name,
    conrelid::regclass AS child_table,
    pg_get_constraintdef(c.oid) AS constraint_definition
FROM
    pg_constraint c
WHERE
    contype = 'f'
ORDER BY
    conrelid::regclass
LIMIT %(limit)s;
""", 
            True,
            "all_foreign_keys" # Data key
        ),
        (
            "Foreign Keys Missing Indexes on Child Table", 
            """
SELECT
    fk.conname AS foreign_key_name,
    n_child.nspname || '.' || fk_table.relname AS child_table,
    ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = fk.conrelid AND attnum = ANY(fk.conkey)) AS fk_col_names,
    n_parent.nspname || '.' || pk_table.relname AS parent_table,
    ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = fk.confrelid AND attnum = ANY(fk.confkey)) AS pk_col_names
FROM
    pg_constraint fk
JOIN
    pg_class fk_table ON fk_table.oid = fk.conrelid
JOIN
    pg_namespace n_child ON n_child.oid = fk_table.relnamespace
JOIN
    pg_class pk_table ON pk_table.oid = fk.confrelid
JOIN
    pg_namespace n_parent ON n_parent.oid = pk_table.relnamespace
WHERE
    fk.contype = 'f'
    AND NOT EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indrelid = fk.conrelid
        AND i.indisvalid AND i.indisready -- Ensure index is valid and ready
        AND array_length(i.indkey, 1) >= array_length(fk.conkey, 1) -- Index must have at least as many columns as FK
        AND (
            -- Check if the foreign key columns are the leading columns of the index
            -- Convert indkey to int[] for robust comparison with conkey (smallint[])
            (SELECT array_agg(x::int) FROM unnest(i.indkey[:array_length(fk.conkey, 1)]) as x) = (SELECT array_agg(x::int) FROM unnest(fk.conkey) as x)
        )
    )
ORDER BY
    child_table, foreign_key_name
LIMIT %(limit)s;
""", 
            True,
            "missing_fk_indexes" # Data key
        )
    ]

    missing_fk_indexes_raw = [] # To store raw details for generating SQL recommendations

    for title, query, condition, data_key in queries:
        if not condition:
            adoc_content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            structured_data[data_key] = {"status": "not_applicable", "reason": "Query not applicable due to condition."}
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        
        # Execute the query and capture the raw results if it's the missing FK index query
        formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(f"{title}\n{formatted_result}")
            structured_data[data_key] = {"status": "error", "details": raw_result}
        else:
            adoc_content.append(title)
            adoc_content.append(formatted_result)
            structured_data[data_key] = {"status": "success", "data": raw_result} # Store raw data
            
            # If this is the missing FK indexes query, store its raw data separately for SQL generation
            if data_key == "missing_fk_indexes":
                missing_fk_indexes_raw = raw_result # This will be a list of dicts

    adoc_content.append("[TIP]\n====\n"
                   "Foreign key constraints enforce referential integrity, but unindexed foreign key columns on the child table can lead to significant write amplification. "
                   "When a row in the parent table is `DELETE`d or `UPDATE`d, PostgreSQL must scan the child table to ensure no referencing rows exist. "
                   "Without an index on the foreign key column(s) in the child table, this becomes a full table scan, consuming excessive I/O and CPU. "
                   "Ensure indexes exist on all foreign key columns in child tables, especially for parent tables that experience frequent `DELETE`s or `UPDATE`s.\n"
                   "====\n")
    
    # Generate SQL statements for missing FK indexes
    if missing_fk_indexes_raw: # Use the raw data collected above
        adoc_content.append("\n==== Recommended SQL for Missing Foreign Key Indexes")
        adoc_content.append("[IMPORTANT]\n====\n"
                       "The following `CREATE INDEX` statements are recommended to improve write performance "
                       "on parent tables with frequently updated/deleted rows, by adding indexes to the "
                       "corresponding foreign key columns in child tables. Always test these changes in a "
                       "staging environment before applying to production.\n"
                       "====\n")
        adoc_content.append("[,sql]\n----")
        for fk_info in missing_fk_indexes_raw:
            # Ensure schema_name and table_name are correctly extracted
            full_child_table_name = fk_info['child_table']
            if '.' in full_child_table_name:
                schema_name, table_name = full_child_table_name.split('.', 1)
            else:
                schema_name = "public" # Default schema
                table_name = full_child_table_name

            fk_col_names = ", ".join(fk_info['fk_col_names'])
            # Generate a more robust index name
            index_name = f"idx_{table_name}_{'_'.join(fk_info['fk_col_names'])}_fk".replace('.', '_').replace('-', '_')
            adoc_content.append(f"CREATE INDEX CONCURRENTLY {index_name} ON {full_child_table_name} ({fk_col_names});")
        adoc_content.append("----")

    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora benefits greatly from properly indexed foreign keys. "
                       "Write amplification due to missing FK indexes can directly contribute to high `WriteIOPS` and `CPUUtilization`. "
                       "Regularly audit your foreign key indexes to maintain optimal write performance.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
