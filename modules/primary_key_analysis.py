def run_primary_key_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes primary keys for potential exhaustion risks, particularly for int2/int4 data types.
    """
    adoc_content = ["=== Primary Key Exhaustion Risk Analysis\n", "Identifies primary keys that may be approaching their maximum values.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Primary key exhaustion analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("-- Check for int2/int4 primary keys with usage statistics")
        adoc_content.append("SELECT ")
        adoc_content.append("    t.table_schema,")
        adoc_content.append("    t.table_name,")
        adoc_content.append("    c.column_name,")
        adoc_content.append("    c.data_type,")
        adoc_content.append("    c.column_default,")
        adoc_content.append("    CASE ")
        adoc_content.append("        WHEN c.data_type = 'smallint' THEN 32767")
        adoc_content.append("        WHEN c.data_type = 'integer' THEN 2147483647")
        adoc_content.append("        WHEN c.data_type = 'bigint' THEN 9223372036854775807")
        adoc_content.append("        ELSE NULL")
        adoc_content.append("    END as max_value,")
        adoc_content.append("    CASE ")
        adoc_content.append("        WHEN c.data_type = 'smallint' THEN 'int2'")
        adoc_content.append("        WHEN c.data_type = 'integer' THEN 'int4'")
        adoc_content.append("        WHEN c.data_type = 'bigint' THEN 'int8'")
        adoc_content.append("        ELSE c.data_type")
        adoc_content.append("    END as pg_type")
        adoc_content.append("FROM information_schema.tables t")
        adoc_content.append("JOIN information_schema.columns c ON t.table_name = c.table_name AND t.table_schema = c.table_schema")
        adoc_content.append("JOIN information_schema.table_constraints tc ON t.table_name = tc.table_name AND t.table_schema = tc.table_schema")
        adoc_content.append("WHERE tc.constraint_type = 'PRIMARY KEY'")
        adoc_content.append("    AND c.column_name = ANY(string_to_array(tc.constraint_definition, ',')")
        adoc_content.append("    AND c.data_type IN ('smallint', 'integer')")
        adoc_content.append("    AND t.table_schema NOT IN ('information_schema', 'pg_catalog')")
        adoc_content.append("ORDER BY t.table_schema, t.table_name;")
        adoc_content.append("----")

    # Query to find primary keys with potential exhaustion risks
    primary_key_query = """
    SELECT 
        t.table_schema,
        t.table_name,
        c.column_name,
        c.data_type,
        c.column_default,
        CASE 
            WHEN c.data_type = 'smallint' THEN 32767
            WHEN c.data_type = 'integer' THEN 2147483647
            WHEN c.data_type = 'bigint' THEN 9223372036854775807
            ELSE NULL
        END as max_value,
        CASE 
            WHEN c.data_type = 'smallint' THEN 'int2'
            WHEN c.data_type = 'integer' THEN 'int4'
            WHEN c.data_type = 'bigint' THEN 'int8'
            ELSE c.data_type
        END as pg_type
    FROM information_schema.tables t
    JOIN information_schema.columns c ON t.table_name = c.table_name AND t.table_schema = c.table_schema
    JOIN information_schema.table_constraints tc ON t.table_name = tc.table_name AND t.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
        AND c.column_name = ANY(string_to_array(tc.constraint_definition, ','))
        AND c.data_type IN ('smallint', 'integer')
        AND t.table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY t.table_schema, t.table_name;
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
            adoc_content.append("\n=== Risk Assessment")
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
            int2_count = sum(1 for row in raw_result if row.get('data_type') == 'smallint')
            int4_count = sum(1 for row in raw_result if row.get('data_type') == 'integer')
            
            adoc_content.append(f"**Summary:**")
            adoc_content.append(f"- Tables with smallint primary keys: {int2_count}")
            adoc_content.append(f"- Tables with integer primary keys: {int4_count}")
            adoc_content.append("")
            
            adoc_content.append("=== Recommendations")
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
    
    adoc_content.append("[TIP]\n====\n")
    adoc_content.append("**Prevention:** Always use BIGINT for primary keys in new tables, even if you don't expect high volume. ")
    adoc_content.append("The storage overhead is minimal, and it prevents future migration headaches.\n")
    adoc_content.append("====\n")
    
    if settings.get('is_aurora', False):
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append("**AWS RDS Aurora:** Primary key migrations may require downtime. ")
        adoc_content.append("Consider using Aurora's fast DDL features and test thoroughly in staging.\n")
        adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data 