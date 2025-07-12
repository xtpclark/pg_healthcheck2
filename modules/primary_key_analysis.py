def run_primary_key_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes primary keys for potential exhaustion risks, particularly for int2/int4 data types.
    Also analyzes sequence exhaustion risk using Crunchy Data's best-practice query.
    """
    adoc_content = ["=== Primary Key Exhaustion Risk Analysis\n", "Identifies primary keys and sequences that may be approaching their maximum values.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # --- Standard PK type analysis (unchanged) ---
    if settings['show_qry'] == 'true':
        adoc_content.append("Primary key exhaustion analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("-- Check for int2/int4 primary keys with usage statistics")
        adoc_content.append("SELECT ")
        adoc_content.append("    n.nspname AS table_schema,")
        adoc_content.append("    c.relname AS table_name,")
        adoc_content.append("    a.attname AS column_name,")
        adoc_content.append("    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,")
        adoc_content.append("    pg_get_expr(ad.adbin, ad.adrelid) AS column_default,")
        adoc_content.append("    CASE ")
        adoc_content.append("        WHEN a.atttypid = 21 THEN 32767")
        adoc_content.append("        WHEN a.atttypid = 23 THEN 2147483647")
        adoc_content.append("        WHEN a.atttypid = 20 THEN 9223372036854775807")
        adoc_content.append("        ELSE NULL")
        adoc_content.append("    END as max_value,")
        adoc_content.append("    CASE ")
        adoc_content.append("        WHEN a.atttypid = 21 THEN 'int2'")
        adoc_content.append("        WHEN a.atttypid = 23 THEN 'int4'")
        adoc_content.append("        WHEN a.atttypid = 20 THEN 'int8'")
        adoc_content.append("        ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)")
        adoc_content.append("    END as pg_type")
        adoc_content.append("FROM pg_class c")
        adoc_content.append("JOIN pg_namespace n ON n.oid = c.relnamespace")
        adoc_content.append("JOIN pg_constraint con ON con.conrelid = c.oid")
        adoc_content.append("JOIN pg_attribute a ON a.attrelid = c.oid")
        adoc_content.append("LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum")
        adoc_content.append("WHERE con.contype = 'p'")
        adoc_content.append("    AND a.attnum = ANY(con.conkey)")
        adoc_content.append("    AND a.atttypid IN (21, 23) -- smallint, integer")
        adoc_content.append("    AND n.nspname NOT IN ('information_schema', 'pg_catalog')")
        adoc_content.append("ORDER BY n.nspname, c.relname;")
        adoc_content.append("----")

    primary_key_query = """
    SELECT 
        n.nspname AS table_schema,
        c.relname AS table_name,
        a.attname AS column_name,
        pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
        pg_get_expr(ad.adbin, ad.adrelid) AS column_default,
        CASE 
            WHEN a.atttypid = 21 THEN 32767
            WHEN a.atttypid = 23 THEN 2147483647
            WHEN a.atttypid = 20 THEN 9223372036854775807
            ELSE NULL
        END as max_value,
        CASE 
            WHEN a.atttypid = 21 THEN 'int2'
            WHEN a.atttypid = 23 THEN 'int4'
            WHEN a.atttypid = 20 THEN 'int8'
            ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)
        END as pg_type
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_constraint con ON con.conrelid = c.oid
    JOIN pg_attribute a ON a.attrelid = c.oid
    LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
    WHERE con.contype = 'p'
        AND a.attnum = ANY(con.conkey)
        AND a.atttypid IN (21, 23) -- smallint, integer
        AND n.nspname NOT IN ('information_schema', 'pg_catalog')
    ORDER BY n.nspname, c.relname;
    """
    
    formatted_result, raw_result = execute_query(primary_key_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Primary Key Exhaustion Analysis\n{formatted_result}")
        structured_data["primary_key_analysis"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("**Tables with Potential Primary Key Exhaustion Risk**")
        adoc_content.append(formatted_result)
        structured_data["primary_key_analysis"] = {"status": "success", "data": raw_result}
        
        # Add analysis and recommendations
        if raw_result:
            adoc_content.append("\n==== Risk Assessment")
            adoc_content.append("The following tables use primary key data types that may exhaust their maximum values:")
            adoc_content.append("")
            adoc_content.append("* **smallint (int2)**: Maximum value 32,767")
            adoc_content.append("* **integer (int4)**: Maximum value 2,147,483,647")
            adoc_content.append("")
            adoc_content.append("**Risk Factors:**")
            adoc_content.append("- High-insert tables may reach limits quickly")
            adoc_content.append("- Auto-incrementing sequences can exhaust rapidly")
            adoc_content.append("- Data migration becomes complex at scale")
            adoc_content.append("")
            
            # Count by data type
            int2_count = sum(1 for row in raw_result if row.get('pg_type') == 'int2')
            int4_count = sum(1 for row in raw_result if row.get('pg_type') == 'int4')
            
            adoc_content.append(f"**Summary:**")
            adoc_content.append(f"- Tables with smallint primary keys: {int2_count}")
            adoc_content.append(f"- Tables with integer primary keys: {int4_count}")
            adoc_content.append("")
            
            adoc_content.append("==== Recommendations")
            adoc_content.append("")
            adoc_content.append("**Immediate Actions:**")
            adoc_content.append("")
            adoc_content.append("1. **Monitor Usage**: Track current maximum values for high-risk tables")
            adoc_content.append("2. **Plan Migration**: Schedule migration to bigint for critical tables")
            adoc_content.append("3. **Test Procedures**: Develop and test migration procedures")
            adoc_content.append("")
            adoc_content.append("**Migration Strategy:**")
            adoc_content.append("")
            adoc_content.append("```sql")
            adoc_content.append("-- Example migration for a table with integer primary key")
            adoc_content.append("-- Step 1: Add new bigint column")
            adoc_content.append("ALTER TABLE your_table ADD COLUMN id_new BIGINT;")
            adoc_content.append("")
            adoc_content.append("-- Step 2: Update new column with existing values")
            adoc_content.append("UPDATE your_table SET id_new = id;")
            adoc_content.append("")
            adoc_content.append("-- Step 3: Drop old primary key constraint")
            adoc_content.append("ALTER TABLE your_table DROP CONSTRAINT your_table_pkey;")
            adoc_content.append("")
            adoc_content.append("-- Step 4: Drop old column and rename new column")
            adoc_content.append("ALTER TABLE your_table DROP COLUMN id;")
            adoc_content.append("ALTER TABLE your_table RENAME COLUMN id_new TO id;")
            adoc_content.append("")
            adoc_content.append("-- Step 5: Add new primary key constraint")
            adoc_content.append("ALTER TABLE your_table ADD PRIMARY KEY (id);")
            adoc_content.append("")
            adoc_content.append("-- Step 6: Update sequence if using SERIAL")
            adoc_content.append("-- (This step varies based on your sequence setup)")
            adoc_content.append("```")
            adoc_content.append("")
            adoc_content.append("**Best Practices:**")
            adoc_content.append("")
            adoc_content.append("- Always use BIGINT for new primary keys")
            adoc_content.append("- Plan migrations during low-traffic periods")
            adoc_content.append("- Test thoroughly in staging environment")
            adoc_content.append("- Monitor application compatibility during migration")
            adoc_content.append("- Update any foreign key references")
            adoc_content.append("")
        else:
            adoc_content.append("\n[NOTE]\n====\nNo tables found with potential primary key exhaustion risks.\nAll primary keys appear to use appropriate data types (bigint recommended).\n====\n")
    
    # --- Sequence exhaustion analysis (Crunchy Data best practice) ---
    adoc_content.append("\n=== Sequence Exhaustion Risk Analysis\n")
    adoc_content.append("This section uses a best-practice query from Crunchy Data to identify auto-incrementing columns and their exhaustion risk.\n")
    adoc_content.append("[NOTE]\n====\nQuery and approach adapted from: https://www.crunchydata.com/blog/the-integer-at-the-end-of-the-universe-integer-overflow-in-postgres\n====\n")
    
    sequence_exhaustion_query = '''
SELECT
    seqs.relname AS sequence,
    format_type(s.seqtypid, NULL) sequence_datatype,
    CONCAT(tbls.relname, '.', attrs.attname) AS owned_by,
    format_type(attrs.atttypid, atttypmod) AS column_datatype,
    pg_sequence_last_value(seqs.oid::regclass) AS last_sequence_value,
    TO_CHAR((
        CASE WHEN format_type(s.seqtypid, NULL) = 'smallint' THEN
            (pg_sequence_last_value(seqs.relname::regclass) / 32767::float)
        WHEN format_type(s.seqtypid, NULL) = 'integer' THEN
            (pg_sequence_last_value(seqs.relname::regclass) / 2147483647::float)
        WHEN format_type(s.seqtypid, NULL) = 'bigint' THEN
            (pg_sequence_last_value(seqs.relname::regclass) / 9223372036854775807::float)
        END) * 100, 'fm9999999999999999999990D00%') AS sequence_percent,
    TO_CHAR((
        CASE WHEN format_type(attrs.atttypid, NULL) = 'smallint' THEN
            (pg_sequence_last_value(seqs.relname::regclass) / 32767::float)
        WHEN format_type(attrs.atttypid, NULL) = 'integer' THEN
            (pg_sequence_last_value(seqs.relname::regclass) / 2147483647::float)
        WHEN format_type(attrs.atttypid, NULL) = 'bigint' THEN
            (pg_sequence_last_value(seqs.relname::regclass) / 9223372036854775807::float)
        END) * 100, 'fm9999999999999999999990D00%') AS column_percent
FROM
    pg_depend d
    JOIN pg_class AS seqs ON seqs.relkind = 'S'
        AND seqs.oid = d.objid
    JOIN pg_class AS tbls ON tbls.relkind = 'r'
        AND tbls.oid = d.refobjid
    JOIN pg_attribute AS attrs ON attrs.attrelid = d.refobjid
        AND attrs.attnum = d.refobjsubid
    JOIN pg_sequence s ON s.seqrelid = seqs.oid
WHERE
    d.deptype = 'a'
    AND d.classid = 1259;
'''
    
    formatted_seq_result, raw_seq_result = execute_query(sequence_exhaustion_query, return_raw=True)
    
    if "[ERROR]" in formatted_seq_result:
        adoc_content.append(f"Sequence Exhaustion Analysis\n{formatted_seq_result}")
        structured_data["sequence_exhaustion_analysis"] = {"status": "error", "details": raw_seq_result}
    else:
        adoc_content.append("[cols=\"1,1,1,1,1,1,1,1\",options=\"header\"]\n|===\n|Sequence|Seq Type|Owned By|Col Type|Last Value|Seq % Used|Col % Used|Status\n")
        high_risk = []
        for row in raw_seq_result:
            seq = row.get('sequence')
            seq_type = row.get('sequence_datatype')
            owned_by = row.get('owned_by')
            col_type = row.get('column_datatype')
            last_val = row.get('last_sequence_value')
            seq_pct = row.get('sequence_percent')
            col_pct = row.get('column_percent')
            # Parse percent as float for threshold
            try:
                seq_pct_val = float(seq_pct.replace('%','')) if seq_pct else 0
                col_pct_val = float(col_pct.replace('%','')) if col_pct else 0
            except Exception:
                seq_pct_val = col_pct_val = 0
            status = "OK"
            if seq_pct_val > 80 or col_pct_val > 80:
                status = "[WARNING] High Risk"
                high_risk.append((seq, owned_by, seq_pct, col_pct))
            adoc_content.append(f"|{seq}|{seq_type}|{owned_by}|{col_type}|{last_val}|{seq_pct}|{col_pct}|{status}\n")
        adoc_content.append("|===\n")
        if high_risk:
            adoc_content.append("\n[WARNING]\n====\n**Sequences/columns above 80% of their maximum value:**\n")
            for seq, owned_by, seq_pct, col_pct in high_risk:
                adoc_content.append(f"* {owned_by} (sequence: {seq}) - Sequence: {seq_pct}, Column: {col_pct}\n")
            adoc_content.append("====\n")
        else:
            adoc_content.append("\n[NOTE]\n====\nNo sequences or columns are above 80% of their maximum value.\n====\n")
        structured_data["sequence_exhaustion_analysis"] = {"status": "success", "data": raw_seq_result}
    
    adoc_content.append("\n[TIP]\n====\n")
    adoc_content.append("**Prevention:** Always use BIGSERIAL or BIGINT for new auto-incrementing columns in high-volume tables. Monitor sequence usage regularly. See [Crunchy Data blog](https://www.crunchydata.com/blog/the-integer-at-the-end-of-the-universe-integer-overflow-in-postgres) for more details.\n")
    adoc_content.append("====\n")
    
    if settings.get('is_aurora', False):
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append("**AWS RDS Aurora:** Primary key and sequence migrations may require downtime. ")
        adoc_content.append("Consider using Aurora's fast DDL features and test thoroughly in staging.\n")
        adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data 
