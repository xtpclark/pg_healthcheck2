def run_suggested_config_values(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Provides suggested configuration values based on workload analysis and hardware characteristics.
    This module analyzes current settings and provides recommendations for optimization.
    """
    adoc_content = ["=== Suggested Configuration Values", "Provides recommended configuration values based on workload and hardware analysis.\n"]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    # Get PostgreSQL version for compatibility
    version_query = "SELECT version();"
    version_result, _ = execute_query(version_query, return_raw=True)
    
    # Extract version number for compatibility checks
    pg_version = None
    if version_result and not isinstance(version_result, str):
        try:
            version_str = version_result[0]['version'] if isinstance(version_result, list) and version_result else str(version_result)
            # Extract version number (e.g., "PostgreSQL 15.3" -> 15)
            import re
            version_match = re.search(r'PostgreSQL (\d+)', version_str)
            if version_match:
                pg_version = int(version_match.group(1))
        except (IndexError, AttributeError, ValueError):
            pg_version = 13  # Default to minimum supported version
    
    # Query for current memory settings
    memory_settings_query = """
        SELECT 
            name, 
            setting, 
            unit, 
            short_desc,
            context,
            category
        FROM pg_settings 
        WHERE name IN (
            'shared_buffers', 'work_mem', 'maintenance_work_mem', 'effective_cache_size',
            'max_connections', 'checkpoint_completion_target', 'wal_buffers',
            'random_page_cost', 'effective_io_concurrency', 'max_worker_processes',
            'max_parallel_workers', 'max_parallel_workers_per_gather'
        )
        ORDER BY category, name;
    """
    
    # Query for system information to calculate recommendations
    system_info_query = """
        SELECT 
            setting as total_memory_mb
        FROM pg_settings 
        WHERE name = 'shared_buffers';
    """
    
    # Query for workload characteristics
    workload_query = """
        SELECT 
            COUNT(*) as total_connections,
            COUNT(CASE WHEN state = 'active' THEN 1 END) as active_connections,
            COUNT(CASE WHEN state = 'idle' THEN 1 END) as idle_connections,
            COUNT(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_transaction
        FROM pg_stat_activity 
        WHERE datname = %(database)s;
    """
    
    # Query for database size information
    db_size_query = """
        SELECT 
            pg_size_pretty(pg_database_size(%(database)s)) as database_size,
            pg_database_size(%(database)s) as database_size_bytes
        FROM pg_database 
        WHERE datname = %(database)s;
    """
    
    # Query for table count and average table size
    table_stats_query = """
        SELECT 
            COUNT(*) as table_count,
            AVG(pg_total_relation_size(c.oid)) as avg_table_size_bytes,
            MAX(pg_total_relation_size(c.oid)) as max_table_size_bytes
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'r' 
            AND n.nspname NOT IN ('information_schema', 'pg_catalog')
            AND c.relname NOT LIKE 'pg_%%';
    """
    
    if settings['show_qry'] == 'true':
        adoc_content.append("Suggested config values analysis queries:")
        adoc_content.append("[,sql]\n----")
        adoc_content.append(memory_settings_query)
        adoc_content.append(system_info_query)
        adoc_content.append(workload_query)
        adoc_content.append(db_size_query)
        adoc_content.append(table_stats_query)
        adoc_content.append("----")

    # Execute current settings analysis
    formatted_settings_result, raw_settings_result = execute_query(memory_settings_query, return_raw=True)
    
    if "[ERROR]" in formatted_settings_result:
        adoc_content.append(f"Current Memory Settings\n{formatted_settings_result}")
        structured_data["current_settings"] = {"status": "error", "details": raw_settings_result}
    else:
        adoc_content.append("==== Current Memory Settings")
        adoc_content.append(formatted_settings_result)
        structured_data["current_settings"] = {"status": "success", "data": raw_settings_result}

    # Execute workload analysis
    params_for_workload = {'database': settings['database']}
    formatted_workload_result, raw_workload_result = execute_query(workload_query, params=params_for_workload, return_raw=True)
    
    if "[ERROR]" in formatted_workload_result:
        adoc_content.append(f"Workload Analysis\n{formatted_workload_result}")
        structured_data["workload_analysis"] = {"status": "error", "details": raw_workload_result}
    else:
        adoc_content.append("==== Workload Analysis")
        adoc_content.append(formatted_workload_result)
        structured_data["workload_analysis"] = {"status": "success", "data": raw_workload_result}

    # Execute database size analysis
    formatted_size_result, raw_size_result = execute_query(db_size_query, params=params_for_workload, return_raw=True)
    
    if "[ERROR]" in formatted_size_result:
        adoc_content.append(f"Database Size Analysis\n{formatted_size_result}")
        structured_data["database_size"] = {"status": "error", "details": raw_size_result}
    else:
        adoc_content.append("==== Database Size Analysis")
        adoc_content.append(formatted_size_result)
        structured_data["database_size"] = {"status": "success", "data": raw_size_result}

    # Execute table statistics
    formatted_table_result, raw_table_result = execute_query(table_stats_query, return_raw=True)
    
    if "[ERROR]" in formatted_table_result:
        adoc_content.append(f"Table Statistics\n{formatted_table_result}")
        structured_data["table_statistics"] = {"status": "error", "details": raw_table_result}
    else:
        adoc_content.append("==== Table Statistics")
        adoc_content.append(formatted_table_result)
        structured_data["table_statistics"] = {"status": "success", "data": raw_table_result}

    # Generate recommendations based on analysis
    adoc_content.append("\n==== Configuration Recommendations")
    
    # Analyze workload characteristics for recommendations
    if raw_workload_result and isinstance(raw_workload_result, list) and raw_workload_result:
        workload_data = raw_workload_result[0]
        total_connections = workload_data.get('total_connections', 0)
        active_connections = workload_data.get('active_connections', 0)
        
        adoc_content.append("\n[TIP]\n====\n**Connection-Based Recommendations:**\n")
        if total_connections > 100:
            adoc_content.append("* **High connection count detected**: Consider connection pooling (PgBouncer)\n")
            adoc_content.append("* **Recommended `max_connections`**: 200-300 (with connection pooling)\n")
        elif total_connections > 50:
            adoc_content.append("* **Medium connection count**: Monitor connection usage patterns\n")
            adoc_content.append("* **Recommended `max_connections`**: 100-150\n")
        else:
            adoc_content.append("* **Low connection count**: Current settings may be adequate\n")
            adoc_content.append("* **Recommended `max_connections`**: 50-100\n")
        
        if active_connections > total_connections * 0.8:
            adoc_content.append("* **High active connection ratio**: Consider read replicas for read-heavy workloads\n")
        adoc_content.append("====\n")

    # Analyze database size for recommendations
    if raw_size_result and isinstance(raw_size_result, list) and raw_size_result:
        size_data = raw_size_result[0]
        db_size_bytes = size_data.get('database_size_bytes', 0)
        
        adoc_content.append("\n[TIP]\n====\n**Size-Based Recommendations:**\n")
        if db_size_bytes > 100 * 1024 * 1024 * 1024:  # > 100GB
            adoc_content.append("* **Large database detected**: Consider partitioning and archiving strategies\n")
            adoc_content.append("* **Recommended `shared_buffers`**: 25% of RAM (minimum 8GB)\n")
            adoc_content.append("* **Recommended `work_mem`**: 64-128MB for complex queries\n")
        elif db_size_bytes > 10 * 1024 * 1024 * 1024:  # > 10GB
            adoc_content.append("* **Medium database**: Optimize for typical OLTP workloads\n")
            adoc_content.append("* **Recommended `shared_buffers`**: 25% of RAM\n")
            adoc_content.append("* **Recommended `work_mem`**: 32-64MB\n")
        else:
            adoc_content.append("* **Small database**: Standard settings should be adequate\n")
            adoc_content.append("* **Recommended `shared_buffers`**: 25% of RAM\n")
            adoc_content.append("* **Recommended `work_mem`**: 16-32MB\n")
        adoc_content.append("====\n")

    # General configuration recommendations
    adoc_content.append("\n[TIP]\n====\n**General Configuration Recommendations:**\n")
    adoc_content.append("* **`shared_buffers`**: 25% of system RAM (minimum 128MB, maximum 8GB)\n")
    adoc_content.append("* **`work_mem`**: 4MB per connection, but limit total to 25% of RAM\n")
    adoc_content.append("* **`maintenance_work_mem`**: 256MB for maintenance operations\n")
    adoc_content.append("* **`effective_cache_size`**: 75% of system RAM\n")
    adoc_content.append("* **`checkpoint_completion_target`**: 0.9 for smooth checkpoint writes\n")
    adoc_content.append("* **`wal_buffers`**: 16MB (or 1/32 of shared_buffers)\n")
    adoc_content.append("* **`random_page_cost`**: 1.1 for SSD, 4.0 for HDD\n")
    adoc_content.append("* **`effective_io_concurrency`**: 200 for SSD, 2 for HDD\n")
    adoc_content.append("====\n")
    
    adoc_content.append("\n[WARNING]\n====\n**Configuration Warnings:**\n")
    adoc_content.append("* **Avoid setting `shared_buffers` > 50% of RAM** - can cause memory pressure\n")
    adoc_content.append("* **Monitor `work_mem` usage** - high values can cause swapping\n")
    adoc_content.append("* **Test configuration changes** in staging before production\n")
    adoc_content.append("* **Use `pg_stat_statements`** to monitor query performance impact\n")
    adoc_content.append("====\n")
    
    if settings['is_aurora'] == 'true':
        adoc_content.append("\n[NOTE]\n====\n**AWS RDS Aurora Considerations:**\n")
        adoc_content.append("* Aurora optimizes many settings automatically\n")
        adoc_content.append("* Focus on application-level optimizations\n")
        adoc_content.append("* Use Aurora-specific parameter groups for customization\n")
        adoc_content.append("* Monitor Aurora-specific metrics in CloudWatch\n")
        adoc_content.append("====\n")
    
    # Add version-specific recommendations
    if pg_version and pg_version >= 14:
        adoc_content.append("\n[NOTE]\n====\n**PostgreSQL 14+ Configuration Features:**\n")
        adoc_content.append("* **Enhanced parallel query processing** - tune `max_parallel_workers`\n")
        adoc_content.append("* **Improved WAL handling** - optimize `wal_buffers` and checkpoint settings\n")
        adoc_content.append("* **Better memory management** - more efficient `shared_buffers` usage\n")
        adoc_content.append("====\n")
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data 
