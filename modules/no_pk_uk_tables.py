def run_no_pk_uk_tables(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Identifies tables that do not have a Primary Key (PK) or a Unique Key (UK).
    Tables without such keys can cause issues with logical replication and
    make data manipulation less reliable.
    """
    adoc_content = ["=== Tables Without Primary or Unique Keys", "Identifies tables lacking Primary Keys or Unique Keys, which can impact replication and data integrity.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Query for tables without PK/UK:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("""
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind = 'r' -- Only regular tables
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    AND NOT EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indrelid = c.oid AND i.indisprimary
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_constraint con
        WHERE con.conrelid = c.oid AND con.contype = 'u'
    )
ORDER BY
    schema_name, table_name
LIMIT %(limit)s;
""")
        adoc_content.append("----")

    query = """
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind = 'r' -- Only regular tables
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    AND NOT EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indrelid = c.oid AND i.indisprimary
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_constraint con
        WHERE con.conrelid = c.oid AND con.contype = 'u'
    )
ORDER BY
    schema_name, table_name
LIMIT %(limit)s;
"""
    
    # Standardized parameter passing pattern:
    params_for_query = {'limit': settings['row_limit']}
    
    formatted_result, raw_result = execute_query(query, params=params_for_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Tables Without Primary or Unique Keys\n{formatted_result}")
        structured_data["tables_without_pk_uk"] = {"status": "error", "details": raw_result}
    elif not raw_result: # No results returned
        adoc_content.append("[NOTE]\n====\nNo tables found without Primary Keys or Unique Keys.\n====\n")
        structured_data["tables_without_pk_uk"] = {"status": "success", "data": []}
    else:
        adoc_content.append("Tables Without Primary or Unique Keys")
        adoc_content.append(formatted_result)
        structured_data["tables_without_pk_uk"] = {"status": "success", "data": raw_result} # Store raw data
    
    adoc_content.append("[TIP]\n====\n"
                   "Tables without a Primary Key (PK) or a Unique Key (UK) can pose significant challenges, "
                   "especially for logical replication. Logical replication requires a unique identifier to "
                   "correctly apply `UPDATE` and `DELETE` operations on the subscriber. Without a PK/UK, "
                   "replication might fall back to less efficient methods (e.g., full table scans based on all columns) "
                   "or even fail. Additionally, PKs/UKs are fundamental for data integrity and efficient query planning.\n"
                   "====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\n"
                       "For AWS RDS Aurora, especially when using logical replication (e.g., for DMS or cross-region replication), "
                       "tables without a primary key are a common source of issues. "
                       "AWS DMS, for instance, requires a primary key or a unique index on a non-nullable column for efficient replication. "
                       "Ensure all tables intended for replication have appropriate unique identifiers.\n"
                       "====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

