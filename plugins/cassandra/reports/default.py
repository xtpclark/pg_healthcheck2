REPORT_SECTIONS = [
    {
        "title": "Default Section",
        "actions": []
    },
    {
        'title': 'Performance Monitoring',
        'actions': [
            {'type': 'module', 'module': 'plugins.cassandra.checks.compaction_statistics', 'function': 'run_compaction_statistics'},
            {'type': 'module', 'module': 'plugins.cassandra.checks.cassandra_node_health', 'function': 'run_cassandra_node_health'},
        ]
    },
    {
        'title': 'Cassandra Node Information',
        'actions': [
            {'type': 'module', 'module': 'plugins.cassandra.checks.check_node_info', 'function': 'run_check_node_info'},
        ]
    },
]

def get_default_report_definition(connector, settings):
    """Returns the report structure."""
    return REPORT_SECTIONS