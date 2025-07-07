def run_aurora_cpu_metrics(cursor, settings, execute_query, execute_pgbouncer):
    content = ["=== Aurora CPU and IOPS Metrics", "Analyzes CPU and IOPS usage to identify saturation issues on the writer node."]
    if settings['show_qry'] == 'true':
        content.append("CPU and IOPS queries:")
        content.append("[,sql]\n----")
        content.append("SELECT replica_lag, replica_lag_size FROM aurora_replica_status();")
        content.append("SELECT state, count(*) FROM pg_stat_activity WHERE state = 'active' GROUP BY state;")
        content.append("SELECT query, calls, total_exec_time, temp_blks_written FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT %(limit)s;")
        content.append("----")

    queries = [
        ("Aurora Replication Metrics", "SELECT replica_lag, replica_lag_size FROM aurora_replica_status();", settings['is_aurora'] == 'true'),
        ("Active Connections", "SELECT state, count(*) FROM pg_stat_activity WHERE state = 'active' GROUP BY state;", True),
        ("Top CPU-Intensive Queries", "SELECT query, calls, total_exec_time, temp_blks_written FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT %(limit)s;", settings['has_pgstat'] == 't')
    ]

    for title, query, condition in queries:
        if not condition:
            content.append(f"{title}\n[NOTE]\n====\n{'Aurora-specific metrics not available.' if 'aurora' in query else 'pg_stat_statements not installed.'}\n====")
            continue
        content.append(title)
        content.append(execute_query(query, params={'limit': settings['row_limit']}))
    
    content.append("[TIP]\n====\nOptimize high-CPU queries or scale up the writer node. Monitor CPUUsage in AWS CloudWatch.\n====")
    return "\n".join(content)
