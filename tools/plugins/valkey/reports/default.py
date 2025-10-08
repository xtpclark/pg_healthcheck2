REPORT_SECTIONS = [
    {
        "title": "Memory and Resource Usage",
        "actions": [
            {'type': 'module', 'module': 'memory_usage', 'function': 'run_memory_usage'},
            {'type': 'module', 'module': 'connected_clients', 'function': 'run_connected_clients'},
            {'type': 'module', 'module': 'memory_pressure', 'function': 'run_memory_pressure_check'}
        ]
    },
    {
        "title": "Keyspace Analysis",
        "actions": [
            {'type': 'module', 'module': 'keyspace_stats', 'function': 'run_keyspace_stats'}
        ]
    },
    {
        "title": "Valkey Health Report",
        "actions": [
            {'type': 'module', 'module': 'replication_status', 'function': 'run_replication_status'}
        ]
    },
    {
        "title": "Persistence and Durability",
        "actions": [
            {'type': 'module', 'module': 'persistence_settings', 'function': 'run_persistence_check'}
        ]
    },
    {
        "title": "Performance Checks",
        "actions": [
            {'type': 'module', 'module': 'cache_hit_ratio', 'function': 'run_cache_hit_ratio'}
        ]
    }
]

def get_default_report_definition(connector, settings):
    """Returns the report structure."""
    return REPORT_SECTIONS