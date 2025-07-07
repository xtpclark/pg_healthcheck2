def run_cache_analysis(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Cache Analysis", "Analyzes PostgreSQL buffer cache usage and hit ratios to identify performance bottlenecks."]
    
    if settings['show_qry'] == 'true':
        content.append("Cache analysis queries:")
        content.append("[,sql]\n----")
        content.append("SELECT datname, blks_hit, blks_read, round((blks_hit::float / (blks_hit + blks_read) * 100)::numeric, 2) AS hit_ratio_percent FROM pg_stat_database WHERE blks_read > 0 AND datname = %(database)s;")
        content.append("SELECT buffers_alloc, buffers_backend, buffers_clean, buffers_checkpoint, checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter;")
        content.append("----")

    queries = [
        ("Database Cache Hit Ratio", "SELECT datname, blks_hit, blks_read, round((blks_hit::float / (blks_hit + blks_read) * 100)::numeric, 2) AS hit_ratio_percent FROM pg_stat_database WHERE blks_read > 0 AND datname = %(database)s;", True),
        ("Buffer Cache Statistics", "SELECT buffers_alloc, buffers_backend, buffers_clean, buffers_checkpoint, checkpoints_timed, checkpoints_req FROM pg_stat_bgwriter;", True)
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\nQuery not applicable.\n====")
            continue
        content.append(title)
        content.append(execute_query(query, params={'database': settings['database'], 'limit': settings['row_limit']}))
    
    content.append("[TIP]\n====\nA cache hit ratio below 90% may indicate insufficient shared_buffers or ineffective query plans. Increase shared_buffers in the RDS parameter group for Aurora or adjust queries to improve cache efficiency. High checkpoints_req values suggest tuning checkpoint_timeout or max_wal_size.\n====")
    if settings['is_aurora'] == 'true':
        content.append("[NOTE]\n====\nFor AWS RDS Aurora, shared_buffers and checkpoint settings are managed via the parameter group. Use AWS Console to adjust these parameters.\n====")
    
    return "\n".join(content)
