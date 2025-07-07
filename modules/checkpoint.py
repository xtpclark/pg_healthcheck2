def run_checkpoint(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Checkpoint Activity", "Analyzes checkpoint activity to optimize WAL performance and reduce I/O load."]
    
    if settings['show_qry'] == 'true':
        content.append("Checkpoint queries:")
        content.append("[,sql]\n----")
        content.append("SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time, buffers_checkpoint FROM pg_stat_bgwriter;")
        content.append("SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('checkpoint_timeout', 'max_wal_size', 'checkpoint_completion_target') ORDER BY name;")
        content.append("----")

    queries = [
        ("Checkpoint Statistics", "SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time, buffers_checkpoint FROM pg_stat_bgwriter;", True),
        ("Checkpoint Configuration", "SELECT name, setting, unit, short_desc FROM pg_settings WHERE name IN ('checkpoint_timeout', 'max_wal_size', 'checkpoint_completion_target') ORDER BY name;", True)
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
    
    content.append("[TIP]\n====\nHigh checkpoints_req values indicate frequent checkpoints, which may increase I/O load. Increase checkpoint_timeout or max_wal_size to reduce checkpoint frequency. For Aurora, adjust these settings via the RDS parameter group to mitigate CPU and IOPS saturation.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nAWS RDS Aurora manages checkpoint settings via the parameter group. Use the AWS Console to adjust checkpoint_timeout or max_wal_size.\n====")
    
    return "\n".join(content)
