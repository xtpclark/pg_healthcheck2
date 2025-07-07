def run_postgres_overview(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== PostgreSQL Overview", "Provides an overview of the PostgreSQL database, including version, uptime, size, and key configuration settings."]
    
    if settings['show_qry'] == 'true':
        content.append("Overview queries:")
        content.append("[,sql]\n----")
        content.append("SELECT version();")
        content.append("SELECT current_database() AS database, pg_size_pretty(pg_database_size(current_database())) AS size;")
        content.append("SELECT setting AS uptime FROM pg_settings WHERE name = 'pg_uptime';")
        content.append("SELECT name, setting, unit FROM pg_settings WHERE name IN ('max_connections', 'work_mem', 'shared_buffers', 'effective_cache_size') ORDER BY name;")
        content.append("----")

    queries = [
        ("Database Version", "SELECT version();", True),
        ("Database Size", "SELECT current_database() AS database, pg_size_pretty(pg_database_size(current_database())) AS size;", True),
        ("Uptime", "SELECT setting AS uptime FROM pg_settings WHERE name = 'pg_uptime';", settings['is_aurora'] == 'true'),
        ("Key Configuration Settings", "SELECT name, setting, unit FROM pg_settings WHERE name IN ('max_connections', 'work_mem', 'shared_buffers', 'effective_cache_size') ORDER BY name;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\n{'Aurora-specific metrics not available.' if 'pg_uptime' in query else 'Query not applicable.'}\n====")
            continue
        content.append(title)
        content.append(execute_query(query, params={'limit': settings['row_limit']}))
    
    content.append("[TIP]\n====\nReview database version for compatibility and upgrades. Check max_connections and work_mem for performance tuning, especially for CPU saturation issues.\n====")
    return "\n".join(content)
