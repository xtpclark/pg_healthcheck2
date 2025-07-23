def run_data_checksums_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes data checksums configuration and provides security recommendations.
    """
    adoc_content = ["=== Data Checksums Analysis\n", "Analyzes data checksums configuration for data integrity and security.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Data checksums analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("-- Check data_checksums setting")
        adoc_content.append("SELECT name, setting, unit, category, short_desc")
        adoc_content.append("FROM pg_settings")
        adoc_content.append("WHERE name = 'data_checksums';")
        adoc_content.append("----")

    # Query to check data_checksums setting
    checksums_query = """
    SELECT name, setting, unit, category, short_desc
    FROM pg_settings
    WHERE name = 'data_checksums';
    """
    
    formatted_result, raw_result = execute_query(checksums_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Data Checksums Configuration\n{formatted_result}")
        structured_data["data_checksums_config"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("**Data Checksums Configuration**")
        adoc_content.append(formatted_result)
        structured_data["data_checksums_config"] = {"status": "success", "data": raw_result}
        
        # Analyze the result
        if raw_result:
            checksums_enabled = any(row.get('setting') == 'on' for row in raw_result)
            
            if checksums_enabled:
                adoc_content.append("\n==== Data Checksums Status: ✅ ENABLED")
                adoc_content.append("")
                adoc_content.append("**Benefits of Data Checksums:**")
                adoc_content.append("- **Data Integrity**: Detects corruption from disk failures, memory errors, or I/O issues")
                adoc_content.append("- **Early Detection**: Identifies problems before they cause application errors")
                adoc_content.append("- **Recovery Confidence**: Ensures backup and restore operations maintain data integrity")
                adoc_content.append("- **Storage Reliability**: Validates data during read operations")
                adoc_content.append("")
                adoc_content.append("**Performance Impact:**")
                adoc_content.append("- Minimal overhead (typically < 1% performance impact)")
                adoc_content.append("- Write operations include checksum calculation")
                adoc_content.append("- Read operations verify checksums automatically")
                adoc_content.append("")
                adoc_content.append("**✅ Configuration Status: Good**")
                adoc_content.append("Data checksums are properly enabled for data integrity protection.")
                adoc_content.append("")
                structured_data["data_corruption"] = {"status": "enabled", "message": "Data checksums enabled"}
                
            else:
                adoc_content.append("\n==== Data Checksums Status: ❌ DISABLED")
                adoc_content.append("")
                adoc_content.append("**⚠️ SECURITY RISK IDENTIFIED**")
                adoc_content.append("")
                adoc_content.append("**Risks of Disabled Data Checksums:**")
                adoc_content.append("- **Silent Data Corruption**: Disk failures may go undetected")
                adoc_content.append("- **Backup Uncertainty**: Cannot verify backup integrity")
                adoc_content.append("- **Application Errors**: Corrupted data may cause unexpected application failures")
                adoc_content.append("- **Recovery Issues**: Corrupted data may prevent successful recovery")
                adoc_content.append("")
                adoc_content.append("**Immediate Recommendations:**")
                adoc_content.append("")
                adoc_content.append("1. **Enable Data Checksums** (requires cluster restart)")
                adoc_content.append("2. **Plan Maintenance Window** for the restart")
                adoc_content.append("3. **Test in Staging** first to ensure compatibility")
                adoc_content.append("4. **Monitor Performance** after enabling")
                adoc_content.append("")
                adoc_content.append("**Enabling Data Checksums:**")
                adoc_content.append("")
                adoc_content.append("```sql")
                adoc_content.append("-- This requires a cluster restart")
                adoc_content.append("-- Add to postgresql.conf:")
                adoc_content.append("data_checksums = on")
                adoc_content.append("")
                adoc_content.append("-- Then restart PostgreSQL")
                adoc_content.append("-- For RDS/Aurora, modify the parameter group")
                adoc_content.append("```")
                adoc_content.append("")
                adoc_content.append("**Important Notes:**")
                adoc_content.append("- Enabling checksums requires a **full cluster restart**")
                adoc_content.append("- **Cannot be enabled on existing data** without pg_checksums tool")
                adoc_content.append("- **New data** will have checksums after restart")
                adoc_content.append("- Consider **pg_checksums** tool for existing data")
                adoc_content.append("")
                structured_data["data_corruption"] = {"status": "disabled", "risk": "high"}
        else:
            adoc_content.append("\n[NOTE]\n====\nUnable to determine data checksums status.\n====\n")
    
    adoc_content.append("[TIP]\n====\n")
    adoc_content.append("**Best Practice:** Always enable data checksums in production environments. ")
    adoc_content.append("The minimal performance overhead is far outweighed by the data integrity benefits.\n")
    adoc_content.append("====\n")
    
    if settings.get('is_aurora', False):
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append("**AWS RDS Aurora:** Data checksums are typically enabled by default. ")
        adoc_content.append("If disabled, enable via the DB cluster parameter group and restart the cluster.\n")
        adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data 
