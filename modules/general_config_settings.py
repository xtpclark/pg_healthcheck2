def run_general_config_settings(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Analyzes a broad range of PostgreSQL configuration settings.
    """
    adoc_content = ["Analyzes a broad range of PostgreSQL configuration settings.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Corrected and expanded category names based on PostgreSQL 15 output
    general_settings_query = """
        SELECT name, setting, unit, category, short_desc
        FROM pg_settings
        WHERE category IN (
            'Connections and Authentication / Connection Settings',
            'Connections and Authentication / Authentication',
            'Connections and Authentication / SSL',
            'Resource Usage / Memory',
            'Query Tuning / Planner Cost Constants',
            'Query Tuning / Other Planner Options',
            'Query Tuning / Planner Method Configuration',
            'Reporting and Logging / What to Log',
            'Reporting and Logging / Where to Log',
            'Reporting and Logging / When to Log',
            'Write-Ahead Log / Settings',
            'Write-Ahead Log / Checkpoints',
            'Resource Usage / Asynchronous Behavior',
            'Resource Usage / Background Writer',
            'Resource Usage / Disk',
            'Resource Usage / Kernel Resources'
        )
        ORDER BY category, name LIMIT %(limit)s;
    """

    if settings['show_qry'] == 'true':
        adoc_content.append("General configuration settings query:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(general_settings_query)
        adoc_content.append("----")

    # Standardized parameter passing pattern: this query uses %(limit)s
    params_for_query = {'limit': settings['row_limit']}
    
    formatted_result, raw_result = execute_query(general_settings_query, params=params_for_query, return_raw=True)
    
    if "[ERROR]" in formatted_result:
        adoc_content.append(f"General Configuration Settings\n{formatted_result}")
        structured_data["general_settings"] = {"status": "error", "details": raw_result}
    else:
        adoc_content.append("General Configuration Settings")
        adoc_content.append(formatted_result)
        structured_data["general_settings"] = {"status": "success", "data": raw_result} # Store raw data
    
    # NEW: Configuration Changes Analysis
    adoc_content.append("\n=== Configuration Changes from Defaults")
    adoc_content.append("The following settings have been modified from their PostgreSQL default values:")
    
    config_changes_query = """
        SELECT 
            name,
            setting as current_value,
            boot_val as default_value,
            CASE 
                WHEN setting != boot_val THEN 'Modified'
                ELSE 'Default'
            END as status,
            unit,
            short_desc
        FROM pg_settings 
        WHERE setting != boot_val 
        AND category IN (
            'Connections and Authentication / Connection Settings',
            'Connections and Authentication / Authentication',
            'Connections and Authentication / SSL',
            'Resource Usage / Memory',
            'Query Tuning / Planner Cost Constants',
            'Query Tuning / Other Planner Options',
            'Query Tuning / Planner Method Configuration',
            'Reporting and Logging / What to Log',
            'Reporting and Logging / Where to Log',
            'Reporting and Logging / When to Log',
            'Write-Ahead Log / Settings',
            'Write-Ahead Log / Checkpoints',
            'Resource Usage / Asynchronous Behavior',
            'Resource Usage / Background Writer',
            'Resource Usage / Disk',
            'Resource Usage / Kernel Resources'
        )
        ORDER BY category, name;
    """
    
    formatted_changes, raw_changes = execute_query(config_changes_query, return_raw=True)
    
    if "[ERROR]" in formatted_changes:
        adoc_content.append("[NOTE]\n====\nUnable to retrieve configuration changes analysis.\n====\n")
        structured_data["config_changes"] = {"status": "error", "details": raw_changes}
    else:
        adoc_content.append(formatted_changes)
        structured_data["config_changes"] = {"status": "success", "data": raw_changes}
        
        # Add summary
        if raw_changes:
            adoc_content.append(f"\n[NOTE]\n====\nFound {len(raw_changes)} settings modified from defaults. Review these changes to ensure they align with your requirements and best practices.\n====\n")
        else:
            adoc_content.append("\n[NOTE]\n====\nNo settings found that differ from PostgreSQL defaults.\n====\n")
    
    adoc_content.append("[TIP]\n====\nReview a broad range of settings to understand the overall configuration. Pay attention to how memory, connections, logging, and WAL settings are configured. Ensure they align with your workload requirements and best practices. For managed services, these are typically managed via parameter groups.\n====\n")
    if settings['is_aurora'] == 'true':
        adoc_content.append("[NOTE]\n====\nFor AWS RDS Aurora, most settings are managed via DB cluster parameter groups. While you can modify many, some are fixed or have Aurora-specific behaviors. Always consult AWS documentation for Aurora-specific parameter guidance.\n====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data

