REPORT_SECTIONS = [
    {
        "title": "Default Section",
        "actions": []
    },
    {
        "title": "Performance Checks",
        "actions": [
            {'type': 'module', 'module': 'high_memory_queries', 'function': 'run_high_memory_queries_check'},
            {'type': 'module', 'module': 'active_queries', 'function': 'run_active_queries_check'},
            {'type': 'module', 'module': 'system_resource_usage', 'function': 'run_system_resource_usage'},
            {'type': 'module', 'module': 'query_log_latency', 'function': 'run_query_log_latency_check'}
        ]
    },
    {
        "title": "Data Integrity",
        "actions": [
            {'type': 'module', 'module': 'data_parts_health', 'function': 'run_data_parts_health'}
        ]
    },
    {
        "title": "Database Health",
        "actions": [
            {'type': 'module', 'module': 'replication_status', 'function': 'run_replication_status'},
            {'type': 'module', 'module': 'system_events', 'function': 'run_system_events_check'},
            {'type': 'module', 'module': 'disk_usage', 'function': 'run_disk_usage'},
            {'type': 'module', 'module': 'zookeeper_status', 'function': 'run_zookeeper_status_check'}
        ]
    }
]

def get_default_report_definition(connector, settings):
    """Returns the report structure."""
    return REPORT_SECTIONS