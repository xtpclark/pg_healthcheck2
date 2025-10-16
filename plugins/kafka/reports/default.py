REPORT_SECTIONS = [
    {
        "title": "Default Section",
        "actions": []
    },
    {
        "title": "Kafka Cluster Health",
        "actions": [
            {'type': 'module', 'module': 'plugins.kafka.checks.broker_availability', 'function': 'run_broker_availability'},
            {'type': 'module', 'module': 'plugins.kafka.checks.under_replicated_partitions', 'function': 'run_under_replicated_partitions'},
        ]
    },
    {
        "title": "Topic Configurations",
        "actions": [
            {'type': 'module', 'module': 'plugins.kafka.checks.topic_configurations', 'function': 'run_topic_configurations'},
        ]
    },
    {
        "title": "Performance Monitoring",
        "actions": [
            {'type': 'module', 'module': 'plugins.kafka.checks.consumer_lag', 'function': 'run_consumer_lag'},
        ]
    },
    {
        'title': 'Replication and ISR Health',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_isr_health', 'function': 'run_check_isr_health'},
        ]
    },
    {
        'title': 'Kafka Storage Health',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.broker_disk_utilization', 'function': 'run_broker_disk_utilization'},
        ]
    },
]

def get_default_report_definition(connector, settings):
    """Returns the report structure."""
    return REPORT_SECTIONS