"""
Query library for the foreign_key_audit check.
"""

def get_missing_fk_indexes_query(connector):
    """
    Returns a query to find foreign keys on child tables that are missing
    a corresponding index on the key column(s). This is a major cause of
    write amplification and locking issues.
    """
    return """
        SELECT
            fk.conname AS foreign_key_name,
            n_child.nspname || '.' || fk_table.relname AS child_table,
            ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = fk.conrelid AND attnum = ANY(fk.conkey)) AS fk_col_names,
            n_parent.nspname || '.' || pk_table.relname AS parent_table,
            ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = fk.confrelid AND attnum = ANY(fk.confkey)) AS pk_col_names
        FROM
            pg_constraint fk
        JOIN pg_class fk_table ON fk_table.oid = fk.conrelid
        JOIN pg_namespace n_child ON n_child.oid = fk_table.relnamespace
        JOIN pg_class pk_table ON pk_table.oid = fk.confrelid
        JOIN pg_namespace n_parent ON n_parent.oid = pk_table.relnamespace
        WHERE
            fk.contype = 'f'
            AND NOT EXISTS (
                SELECT 1
                FROM pg_index i
                WHERE i.indrelid = fk.conrelid
                -- Ensure the leading columns of the index match the foreign key columns
                AND (i.indkey::int[] @> fk.conkey::int[] AND i.indkey::int[] <@ fk.conkey::int[])
            )
        ORDER BY
            child_table, foreign_key_name
        LIMIT %(limit)s;
    """

def get_fk_summary_query(connector):
    """
    Returns a query that provides a summary of foreign key health,
    counting total FKs and those missing an index.
    """
    return """
        SELECT
            COUNT(*) AS total_foreign_keys,
            COUNT(*) FILTER (
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM pg_index i
                    WHERE i.indrelid = c.conrelid
                    AND (i.indkey::int[] @> c.conkey::int[] AND i.indkey::int[] <@ c.conkey::int[])
                )
            ) AS unindexed_foreign_keys
        FROM pg_constraint c
        WHERE c.contype = 'f';
    """
