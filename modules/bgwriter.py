def run_bgwriter(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Background Writer Statistics", "Analyzes background writer activity to optimize buffer management and reduce I/O load."]
    
    if settings['show_qry'] == 'true':
        content.append("Background writer queries:")
        content.append("[,sql]\n----")
        content.append("SELECT buffers_clean, maxwritten_clean, buffers_backend, buffers_alloc, buffers_backend_fsync FROM pg_stat_bgwriter;")
        content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('bgwriter_lru_maxpages', 'bgwriter_lru_multiplier', 'bgwriter_delay') ORDER BY name;")
        content.append("----")

    queries = [
        ("Background Writer Metrics", "SELECT buffers_clean, maxwritten_clean, buffers_backend, buffers_alloc, buffers_backend_fsync FROM pg_stat_bgwriter;", True),
        ("Background Writer Configuration", "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('bgwriter_lru_maxpages', 'bgwriter_lru_multiplier', 'bgwriter_delay') ORDER BY name;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        params = None  # No named placeholders in these queries
        result = execute_query(query, params=params)
        if "[ERROR]" in result or "[NOTE]" in result:
            content.append(f"{title}\n{result}")
        else:
            content.append(title)
            content.append(result)
    
    content.append("[TIP]\n====\nHigh buffers_backend or buffers_backend_fsync values indicate heavy backend writes, increasing I/O load. Adjust bgwriter_lru_maxpages or reduce bgwriter_delay for more aggressive cleaning. For Aurora, tune these settings via the RDS parameter group to mitigate CPU and IOPS saturation.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora manages background writer settings via the parameter group. Use the AWS Console to adjust bgwriter_lru_maxpages or bgwriter_delay.\n====")
    
    return "\n".join(content)
