def run_table_constraint_analysis(connector, settings):
    """
    Identifies tables that are missing a Primary Key (PK), which is essential
    for data integrity and efficient row identification. This check now excludes
    table partitions from the analysis.
    """
    adoc_content = ["=== Table Constraint Analysis (Missing Primary Keys)", "Checks for tables lacking a Primary Key. Every table should have a primary key to uniquely identify rows, which is critical for data integrity, replication, and performance.\n"]
    structured_data = {}

    # This query finds user tables that do not have a primary key constraint.
    # It now excludes partitions by checking the `relispartition` flag.
    no_pk_query = """
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        pg_size_pretty(pg_total_relation_size(c.oid)) as table_size,
        c.relispartition AS is_partition
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND c.relispartition = false -- Exclude partitions from the check
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
      AND NOT EXISTS (
          SELECT 1 FROM pg_constraint con
          WHERE con.conrelid = c.oid AND con.contype = 'p'
      )
    ORDER BY pg_total_relation_size(c.oid) DESC;
    """

    try:
        if settings.get('show_qry') == 'true':
            adoc_content.append("Missing primary key query:")
            adoc_content.append("[,sql]\n----")
            adoc_content.append(no_pk_query)
            adoc_content.append("----")

        formatted_result, raw_result = connector.execute_query(no_pk_query, return_raw=True)

        if "[ERROR]" in formatted_result:
            adoc_content.append(formatted_result)
            structured_data["no_pk_tables"] = {"status": "error", "details": raw_result}
        elif not raw_result:
            adoc_content.append("[NOTE]\n====\nAll regular tables have a Primary Key. This is excellent for data integrity.\n====\n")
            structured_data["no_pk_tables"] = {"status": "success", "data": []}
        else:
            adoc_content.append("[IMPORTANT]\n====\nThe following tables are missing a Primary Key. This can lead to duplicate rows and makes it difficult for applications and logical replication tools to reliably identify and modify specific rows. You should add a Primary Key to these tables as soon as possible.\n====\n")
            adoc_content.append(formatted_result)
            structured_data["no_pk_tables"] = {"status": "success", "data": raw_result}

    except Exception as e:
        error_msg = f"Failed during table constraint analysis: {e}"
        adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["no_pk_tables"] = {"status": "error", "details": error_msg}

    adoc_content.append("\n[TIP]\n====\nTo add a primary key, use the `ALTER TABLE <table_name> ADD PRIMARY KEY (<column_name>);` command. If no single column is unique, you can create a composite primary key from multiple columns. If no natural key exists, consider adding an identity column or a column with a `serial` or `bigserial` data type.\n====\n")

    return "\n".join(adoc_content), structured_data
