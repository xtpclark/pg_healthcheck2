def run_settings(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== PostgreSQL Settings", "Analyzes key PostgreSQL configuration settings to identify potential tuning opportunities for performance and stability."]
    
    if settings['show_qry'] == 'true':
        content.append("Settings queries:")
        content.append("[,sql]\n----")
        content.append("SELECT name, setting, unit, category, short_desc FROM pg_settings WHERE category IN ('Connections and Authentication', 'Memory Settings', 'Query Tuning') ORDER BY category, name LIMIT %(limit)s;")
        content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('max_connections', 'work_mem', 'shared_buffers', 'effective_cache_size', 'maintenance_work_mem', 'autovacuum', 'checkpoint_timeout', 'wal_level', 'max_wal_size') ORDER BY name;")
        content.append("----")

    queries = [
        ("Connection and Memory Settings", "SELECT name, setting, unit, category, short_desc FROM pg_settings WHERE category IN ('Connections and Authentication', 'Memory Settings', 'Query Tuning') ORDER BY category, name LIMIT %(limit)s;", True),
        ("Critical Performance Settings", "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('max_connections', 'work_mem', 'shared_buffers', 'effective_cache_size', 'maintenance_work_mem', 'autovacuum', 'checkpoint_timeout', 'wal_level', 'max_wal_size') ORDER BY name;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        content.append(title)
        content.append(execute_query(query, params={'limit': settings['row_limit']}))
    
    content.append("[TIP]\n====\nReview max_connections to ensure sufficient connection slots for your workload, especially for Aurora CPU saturation issues. Adjust work_mem and shared_buffers to balance memory usage and performance. Enable autovacuum if disabled to prevent bloat.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nFor AWS RDS Aurora, some settings (e.g., shared_buffers) are managed by the parameter group. Use AWS Console to adjust these parameters.\n====")
    
    return "\n".join(content)
