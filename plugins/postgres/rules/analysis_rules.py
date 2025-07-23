# --- Configuration for Metric Analysis ---
# PostgreSQL Rules
METRIC_ANALYSIS_CONFIG = {
    # --- Rule for Hot Query Workload Concentration ---
    'query_workload_concentration': {
        'metric_keywords': ['hot_query_summary'],
        'data_conditions': [{'key': 'total_queries_tracked', 'exists': True}],
        'rules': [
            {
                'expression': (
                    "data.get('total_execution_time_all_queries_ms') and data['total_execution_time_all_queries_ms'] > 0 and "
                    "(sum(q.get('total_exec_time', q.get('total_time', 0)) or 0 for q in all_structured_findings.get('hot_queries', {}).get('data', {}).get('top_hot_queries', {}).get('data', [])) / data['total_execution_time_all_queries_ms']) * 100 > 75"
                ),
                'level': 'critical',
                'score': 5,
                'reasoning': "High workload concentration detected. The top {settings['row_limit']} queries account for more than 75% of the total database execution time.",
                'recommendations': ["Focus optimization efforts on the top queries, as this will yield the most significant performance improvements."]
            }
        ]
    },
    'connection_usage': {
        'metric_keywords': ['connection'],
        'data_conditions': [{'key': 'total_connections', 'exists': True}, {'key': 'max_connections', 'exists': True}],
        'rules': [
            {'expression': "not settings.get('using_connection_pooler', False) and (int(data['total_connections']) / int(data['max_connections'])) * 100 > 90", 'level': 'critical', 'score': 5, 'reasoning': "Connection usage at {(int(data['total_connections']) / int(data['max_connections'])) * 100:.1f}% of maximum", 'recommendations': ["Immediate action required: Connection pool near capacity"]},
            {'expression': "not settings.get('using_connection_pooler', False) and (int(data['total_connections']) / int(data['max_connections'])) * 100 > 75", 'level': 'high', 'score': 4, 'reasoning': "Connection usage at {(int(data['total_connections']) / int(data['max_connections'])) * 100:.1f}% of maximum", 'recommendations': ["Monitor connection usage and consider connection pooling"]}
        ]
    },
    'long_running_queries': {
        'metric_keywords': ['query', 'statements'],
        'data_conditions': [{'key': 'total_exec_time', 'exists': True}],
        'rules': [
            {'expression': "float(data['total_exec_time']) > 3600000", 'level': 'critical', 'score': 5, 'reasoning': "Query with {float(data['total_exec_time']) / 1000:.1f}s total execution time", 'recommendations': ["Optimize or terminate long-running queries"]},
            {'expression': "float(data['total_exec_time']) > 600000", 'level': 'high', 'score': 4, 'reasoning': "Query with {float(data['total_exec_time']) / 1000:.1f}s total execution time", 'recommendations': ["Investigate query performance"]}
        ]
    },
    'unused_indexes': {
        'metric_keywords': ['index'],
        'data_conditions': [{'key': 'idx_scan', 'exists': True}],
        'rules': [
            {'expression': "int(data['idx_scan']) == 0", 'level': 'medium', 'score': 3, 'reasoning': "Found potentially unused index: {data['index_name']}", 'recommendations': ["Review index usage on all replicas before removal"]}
        ]
    },
    'vacuum_bloat': {
        'metric_keywords': ['bloated_tables'],
        'data_conditions': [{'key': 'n_dead_tup', 'exists': True}, {'key': 'n_live_tup', 'exists': True}],
        'rules': [
            {'expression': "int(data['n_live_tup']) > 0 and (int(data['n_dead_tup']) / (int(data['n_dead_tup']) + int(data['n_live_tup']))) > 0.5", 'level': 'critical', 'score': 5, 'reasoning': "Critically high dead tuple ratio in table {data.get('relname', 'N/A')}", 'recommendations': ["Immediate VACUUM required"]},
            {'expression': "int(data['n_live_tup']) > 0 and (int(data['n_dead_tup']) / (int(data['n_dead_tup']) + int(data['n_live_tup']))) > 0.2", 'level': 'high', 'score': 4, 'reasoning': "High dead tuple ratio in table {data.get('relname', 'N/A')}", 'recommendations': ["Schedule VACUUM to prevent bloat"]}
        ]
    },
    'systemic_bloat': {
        'metric_keywords': ['bloat_summary'],
        'data_conditions': [{'key': 'tables_with_high_bloat', 'exists': True}],
        'rules': [
            {'expression': "int(data['tables_with_critical_bloat']) > 5", 'level': 'critical', 'score': 5, 'reasoning': "Systemic bloat detected: {data['tables_with_critical_bloat']} tables have critical bloat levels (>50%).", 'recommendations': ["Global autovacuum settings are likely misconfigured for the workload. Review and tune immediately."]},
            {'expression': "int(data['tables_with_high_bloat']) > 10", 'level': 'high', 'score': 4, 'reasoning': "Systemic bloat detected: {data['tables_with_high_bloat']} tables have high bloat levels (>20%).", 'recommendations': ["Global autovacuum settings may need tuning. Investigate workload patterns."]}
        ]
    },
    'aws_cpu_utilization': {
        'metric_keywords': ['CPUUtilization'],
        'data_conditions': [{'key': 'value', 'exists': True}],
        'rules': [
            {'expression': "float(data['value']) > 90", 'level': 'critical', 'score': 5, 'reasoning': "CPU Utilization is critically high at {data['value']:.1f}%.", 'recommendations': ["Investigate top queries, consider scaling instance class."]},
            {'expression': "float(data['value']) > 75", 'level': 'high', 'score': 4, 'reasoning': "CPU Utilization is high at {data['value']:.1f}%.", 'recommendations': ["Monitor CPU usage and optimize resource-intensive queries."]}
        ]
    },
    'aws_free_storage': {
        'metric_keywords': ['FreeStorageSpace'],
        'data_conditions': [{'key': 'value', 'exists': True}],
        'rules': [
            {'expression': "float(data['value']) < 10 * 1024**3", 'level': 'critical', 'score': 5, 'reasoning': "Free storage space is critically low at {data['value'] / 1024**3:.2f} GB.", 'recommendations': ["Increase storage volume immediately to prevent outage."]},
            {'expression': "float(data['value']) < 25 * 1024**3", 'level': 'high', 'score': 4, 'reasoning': "Free storage space is low at {data['value'] / 1024**3:.2f} GB.", 'recommendations': ["Plan to increase storage volume soon."]}
        ]
    },
    'aws_burst_balance': {
        'metric_keywords': ['BurstBalance'],
        'data_conditions': [{'key': 'value', 'exists': True}],
        'rules': [
            {'expression': "float(data['value']) < 10", 'level': 'high', 'score': 4, 'reasoning': "Storage burst balance is low at {data['value']:.1f}%, performance may be throttled.", 'recommendations': ["Consider switching to Provisioned IOPS (io1) or gp3 storage if performance is impacted."]}
        ]
    },
    'rds_proxy_pinning': {
        'metric_keywords': ['ConnectionPinning'],
        'data_conditions': [{'key': 'value', 'exists': True}],
        'rules': [
            {'expression': "float(data['value']) > 5", 'level': 'high', 'score': 4, 'reasoning': "RDS Proxy is experiencing connection pinning ({data['value']:.1f}%), reducing pooler efficiency.", 'recommendations': ["Investigate application queries for session-level settings that cause pinning."]}
        ]
    }
}
