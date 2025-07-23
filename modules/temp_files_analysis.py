def run_temp_files_analysis(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes temporary file usage and work_mem configuration for performance optimization.
    """
    adoc_content = ["=== Temp Files\n", "Analyzes temporary file usage\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Temp files analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append("-- Check temp files usage")
        adoc_content.append("SELECT datname, temp_files, temp_bytes")
        adoc_content.append("FROM pg_stat_database")
        adoc_content.append("WHERE datname = %(database)s;")
        adoc_content.append("")
        adoc_content.append("-- Check work_mem setting")
        adoc_content.append("SELECT name, setting, unit, category, short_desc")
        adoc_content.append("FROM pg_settings")
        adoc_content.append("WHERE name = 'work_mem';")
        adoc_content.append("")
        adoc_content.append("-- Check log_temp_files setting")
        adoc_content.append("SELECT name, setting, unit, category, short_desc")
        adoc_content.append("FROM pg_settings")
        adoc_content.append("WHERE name = 'log_temp_files';")
        adoc_content.append("----")

    # Query to check temp files usage
    temp_files_query = """
    SELECT datname, temp_files, temp_bytes, 
           pg_size_pretty(temp_bytes) as temp_bytes_pretty
    FROM pg_stat_database
    WHERE datname = %(database)s;
    """
    
    params_for_query = {'database': settings['database']}
    formatted_result, raw_result = execute_query(temp_files_query, params=params_for_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"Temporary Files Usage\n{formatted_result}")
        structured_data["temp_files_usage"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("**Temporary Files Usage**")
        adoc_content.append(formatted_result)
        structured_data["temp_files_usage"] = {"status": "success", "data": raw_result}
        
        # Analyze temp files usage
        if raw_result:
            temp_files_count = raw_result[0].get('temp_files', 0) if raw_result else 0
            temp_bytes = raw_result[0].get('temp_bytes', 0) if raw_result else 0
            
            if temp_files_count > 0:
                adoc_content.append(f"\n[WARNING]\n====\nAnalysis: TEMP FILES DETECTED")
                adoc_content.append("")
                adoc_content.append(f"**Current Usage:**")
                adoc_content.append(f"- Temporary files created: {temp_files_count}")
                adoc_content.append(f"- Total temp file size: {raw_result[0].get('temp_bytes_pretty', 'N/A')}")
                adoc_content.append("")
                adoc_content.append("**Performance Impact:**")
                adoc_content.append("- Disk I/O instead of memory operations")
                adoc_content.append("- Slower query execution")
                adoc_content.append("- Increased disk space usage")
                adoc_content.append("- Potential disk I/O bottlenecks")
                adoc_content.append("\n====\n")
            else:
                adoc_content.append(f"\n[INFO]\n====\nAnalysis: NO TEMP FILES DETECTED")
                adoc_content.append("")
                adoc_content.append("**Good Performance:**")
                adoc_content.append("- All operations are using memory")
                adoc_content.append("- No disk spills detected")
                adoc_content.append("- Optimal query performance")
                adoc_content.append("\n====\n")
    
    # Check work_mem setting
    work_mem_query = """
    SELECT name, setting, unit, category, short_desc
    FROM pg_settings
    WHERE name = 'work_mem';
    """
    
    work_mem_result, work_mem_raw = execute_query(work_mem_query, return_raw=True)
    
    if "[ERROR]" not in work_mem_result:
        adoc_content.append("=== Work Memory\n")
        adoc_content.append("**Work Memory Configuration**")
        adoc_content.append(work_mem_result)
        structured_data["work_mem_config"] = {"status": "success", "data": work_mem_raw}
        
        # Analyze work_mem setting
        if work_mem_raw:
            work_mem_setting = work_mem_raw[0].get('setting', '0') if work_mem_raw else '0'
            work_mem_unit = work_mem_raw[0].get('unit', '') if work_mem_raw else ''
            
            # Convert to bytes for analysis
            work_mem_bytes = 0
            try:
                if work_mem_unit == '8kB':
                    work_mem_bytes = int(work_mem_setting) * 8192
                elif work_mem_unit == 'kB':
                    work_mem_bytes = int(work_mem_setting) * 1024
                elif work_mem_unit == 'MB':
                    work_mem_bytes = int(work_mem_setting) * 1024 * 1024
                elif work_mem_unit == 'GB':
                    work_mem_bytes = int(work_mem_setting) * 1024 * 1024 * 1024
                else:
                    work_mem_bytes = int(work_mem_setting)
            except:
                work_mem_bytes = 0
            
            adoc_content.append(f"\n=== Work Memory Analysis")
            adoc_content.append("")
            
            if work_mem_bytes < 4 * 1024 * 1024:  # Less than 4MB
                adoc_content.append("**Status: ⚠️ LOW WORK MEMORY**")
                adoc_content.append("")
                adoc_content.append("**Recommendations:**")
                adoc_content.append("- Increase work_mem to reduce temp file usage")
                adoc_content.append("- Consider 16MB to 64MB for typical workloads")
                adoc_content.append("- Monitor memory usage after changes")
                adoc_content.append("")
            elif work_mem_bytes < 64 * 1024 * 1024:  # Less than 64MB
                adoc_content.append("**Status: ✅ ADEQUATE WORK MEMORY**")
                adoc_content.append("")
                adoc_content.append("**Current setting appears reasonable for most workloads.**")
                adoc_content.append("")
            else:
                adoc_content.append("**Status: ✅ GENEROUS WORK MEMORY**")
                adoc_content.append("")
                adoc_content.append("**High work_mem setting detected.**")
                adoc_content.append("Monitor memory usage to ensure it's not excessive.")
                adoc_content.append("")
    else:
        adoc_content.append(f"Work Memory Configuration\n{work_mem_result}")
        structured_data["work_mem_config"] = {"status": "error", "details": work_mem_raw}
    
    # Check log_temp_files setting
    log_temp_query = """
    SELECT name, setting, unit, category, short_desc
    FROM pg_settings
    WHERE name = 'log_temp_files';
    """
    
    log_temp_result, log_temp_raw = execute_query(log_temp_query, return_raw=True)
    
    if "[ERROR]" not in log_temp_result:
        adoc_content.append("**Temp Files Logging Configuration**")
        adoc_content.append(log_temp_result)
        structured_data["log_temp_config"] = {"status": "success", "data": log_temp_raw}
        
        # Analyze log_temp_files setting
        if log_temp_raw:
            log_temp_setting = log_temp_raw[0].get('setting', '0') if log_temp_raw else '0'
            
            adoc_content.append(f"\n=== Temp Files Logging Analysis")
            adoc_content.append("")
            
            if log_temp_setting == '0':
                adoc_content.append("**Status: ⚠️ TEMP FILES NOT LOGGED**")
                adoc_content.append("")
                adoc_content.append("**Recommendation:** Enable temp files logging to monitor performance issues.")
                adoc_content.append("")
                adoc_content.append("**To enable:**")
                adoc_content.append("```sql")
                adoc_content.append("-- Set to log temp files larger than 0 bytes")
                adoc_content.append("log_temp_files = 0")
                adoc_content.append("")
                adoc_content.append("-- Or set to log files larger than 1MB")
                adoc_content.append("log_temp_files = 1024")
                adoc_content.append("```")
                adoc_content.append("")
            else:
                adoc_content.append("**Status: ✅ TEMP FILES LOGGING ENABLED**")
                adoc_content.append("")
                adoc_content.append(f"Temp files larger than {log_temp_setting} bytes will be logged.")
                adoc_content.append("Monitor logs for temp file usage patterns.")
                adoc_content.append("")
    else:
        adoc_content.append(f"Temp Files Logging Configuration\n{log_temp_result}")
        structured_data["log_temp_config"] = {"status": "error", "details": log_temp_raw}
    
    # Add recommendations
    if raw_result and temp_files_count > 0:
        adoc_content.append("=== Performance Recommendations")
        adoc_content.append("")
        adoc_content.append("**Immediate Actions:**")
        adoc_content.append("")
        adoc_content.append("1. **Increase work_mem**: Reduce temp file usage")
        adoc_content.append("2. **Optimize queries**: Review queries that create temp files")
        adoc_content.append("3. **Add indexes**: Reduce sorting and hashing operations")
        adoc_content.append("4. **Enable logging**: Monitor temp file patterns")
        adoc_content.append("")
        adoc_content.append("**Work Memory Tuning:**")
        adoc_content.append("")
        adoc_content.append("```sql")
        adoc_content.append("-- Example work_mem settings")
        adoc_content.append("work_mem = 16MB  -- For light workloads")
        adoc_content.append("work_mem = 32MB  -- For moderate workloads")
        adoc_content.append("work_mem = 64MB  -- For heavy workloads")
        adoc_content.append("")
        adoc_content.append("-- Enable temp file logging")
        adoc_content.append("log_temp_files = 0  -- Log all temp files")
        adoc_content.append("```")
        adoc_content.append("")
        adoc_content.append("**Monitoring Queries:**")
        adoc_content.append("")
        adoc_content.append("```sql")
        adoc_content.append("-- Check temp file usage by database")
        adoc_content.append("SELECT datname, temp_files, temp_bytes")
        adoc_content.append("FROM pg_stat_database;")
        adoc_content.append("")
        adoc_content.append("-- Check for active temp file creation")
        adoc_content.append("SELECT pid, query_start, query")
        adoc_content.append("FROM pg_stat_activity")
        adoc_content.append("WHERE state = 'active';")
        adoc_content.append("```")
        adoc_content.append("")
    
    adoc_content.append("[TIP]\n====\n")
    adoc_content.append("**Best Practice:** Monitor temp file usage regularly. ")
    adoc_content.append("High temp file usage often indicates queries that could benefit from optimization or indexing.\n")
    adoc_content.append("====\n")
    
    if settings.get('is_aurora', False):
        adoc_content.append("[NOTE]\n====\n")
        adoc_content.append("**AWS RDS Aurora:** Work memory and temp file settings are managed via parameter groups. ")
        adoc_content.append("Monitor Aurora-specific metrics like `ReadIOPS` and `WriteIOPS` for temp file impact.\n")
        adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data 
