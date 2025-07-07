def run_foreign_key_audit(cursor, settings, execute_query, execute_pgbouncer):
    """
    Performs an audit of foreign keys, focusing on identifying potential
    write-amplification issues, especially due to missing indexes on FK columns.
    """
    content = ["=== Foreign Key Audit", "Audits foreign key constraints to identify potential write-amplification issues and ensure data integrity."]
    
    if settings['show_qry'] == 'true':
        content.append("Foreign key audit queries:")
        content.append("[,sql]\n----")
        content.append("""
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
        content.append("""
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
        content.append("----")

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
            True
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
            True
        )
    ]

    missing_fk_indexes = [] # To store details for generating SQL recommendations

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====\n")
            continue
        
        # Standardized parameter passing pattern:
        params_for_query = {'limit': settings['row_limit']} if '%(limit)s' in query else None
        
        # Execute the query and capture the raw results if it's the missing FK index query
        if title == "Foreign Keys Missing Indexes on Child Table":
            # Execute the query directly to get raw results for processing
            cursor.execute(query, params_for_query)
            raw_results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            if not raw_results:
                content.append("[NOTE]\n====\nNo results returned.\n====\n")
            else:
                # Format for display in the report
                table = ['|===', '|' + '|'.join(columns)]
                for row in raw_results:
                    # Store relevant info for SQL generation
                    row_dict = dict(zip(columns, row))
                    missing_fk_indexes.append(row_dict)
                    table.append('|' + '|'.join(str(v) for v in row))
                table.append('|===')
                content.append('\n'.join(table))
        else:
            result = execute_query(query, params=params_for_query)
            if "[ERROR]" in result or "[NOTE]" in result:
                content.append(f"{title}\n{result}")
            else:
                content.append(title)
                content.append(result)
    
    content.append("[TIP]\n====\n"
                   "Foreign key constraints enforce referential integrity, but unindexed foreign key columns on the child table can lead to significant write amplification. "
                   "When a row in the parent table is `DELETE`d or `UPDATE`d, PostgreSQL must scan the child table to ensure no referencing rows exist. "
                   "Without an index on the foreign key column(s) in the child table, this becomes a full table scan, consuming excessive I/O and CPU. "
                   "Ensure indexes exist on all foreign key columns in child tables, especially for parent tables that experience frequent `DELETE`s or `UPDATE`s.\n"
                   "====\n")
    
    # Generate SQL statements for missing FK indexes
    if missing_fk_indexes:
        content.append("\n=== Recommended SQL for Missing Foreign Key Indexes")
        content.append("[IMPORTANT]\n====\n"
                       "The following `CREATE INDEX` statements are recommended to improve write performance "
                       "on parent tables with frequently updated/deleted rows, by adding indexes to the "
                       "corresponding foreign key columns in child tables. Always test these changes in a "
                       "staging environment before applying to production.\n"
                       "====\n")
        content.append("[,sql]\n----")
        for fk_info in missing_fk_indexes:
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
            content.append(f"CREATE INDEX CONCURRENTLY {index_name} ON {full_child_table_name} ({fk_col_names});")
        content.append("----")

    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\n"
                       "AWS RDS Aurora benefits greatly from properly indexed foreign keys. "
                       "Write amplification due to missing FK indexes can directly contribute to high `WriteIOPS` and `CPUUtilization`. "
                       "Regularly audit your foreign key indexes to maintain optimal write performance.\n"
                       "====\n")
    
    return "\n".join(content)
