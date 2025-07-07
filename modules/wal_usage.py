def run_wal_usage(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== WAL Usage and Archiving", "Analyzes Write-Ahead Log (WAL) usage and archiving status to optimize performance and ensure reliable recovery."]
    
    if settings['show_qry'] == 'true':
        content.append("WAL usage queries:")
        content.append("[,sql]\n----")
        content.append("SELECT archived_count, failed_count, last_archived_wal, last_archived_time, last_failed_wal, last_failed_time FROM pg_stat_archiver;")
        content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('wal_level', 'archive_mode', 'archive_timeout', 'max_wal_size') ORDER BY name;")
        content.append("----")

    queries = [
        ("WAL Archiving Status", "SELECT archived_count, failed_count, last_archived_wal, last_archived_time, last_failed_wal, last_failed_time FROM pg_stat_archiver;", True),
        ("WAL Configuration Settings", "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('wal_level', 'archive_mode', 'archive_timeout', 'max_wal_size') ORDER BY name;", True)
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
    
    content.append("[TIP]\n====\nEnsure archive_mode is 'on' and monitor failed_count for archiving issues. Adjust archive_timeout to balance WAL segment creation and I/O load. For Aurora, verify WAL archiving setup via the RDS parameter group and CloudWatch metrics (e.g., WriteIOPS) to address CPU saturation.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora manages WAL archiving internally. Use the AWS Console to configure archive settings and monitor WriteIOPS in CloudWatch for performance insights.\n====")
    
    return "\n".join(content)
