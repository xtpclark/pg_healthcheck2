REPORT_SECTIONS = [
    {
        "title": "Default Section",
        "actions": []
    },
    {
        "title": "Kafka Cluster Health",
        "actions": [
            {'type': 'module', 'module': 'plugins.kafka.checks.under_replicated_partitions', 'function': 'run_under_replicated_partitions'},
            {'type': 'module', 'module': 'plugins.kafka.checks.check_partition_balance', 'function': 'run_partition_balance'},
        ]
    },
    {
        "title": "Topic Configurations",
        "actions": [
        ]
    },
    {
        'title': 'Topic Management',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_topic_count_and_naming', 'function': 'run_check_topic_count_and_naming'},
        ]
    },
    {
        "title": "Performance Monitoring",
        "actions": [
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
            {'type': 'module', 'module': 'plugins.kafka.checks.check_storage_health', 'function': 'run_check_storage_health'},
        ]
    },
    {
        'title': 'Broker Availability',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_broker_availability', 'function': 'run_check_broker_availability'},

        ]
    },
    {
        'title': 'Consumer Groups',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_consumer_group_health', 'function': 'run_check_consumer_group_health'},
        ]
    },
    {
        'title': '[Consumer Group Health]',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.down_members', 'function': 'run_down_members'},
        ]
    },
    {
        'title': '[Consumer Health]',
        'actions': [
            {'type': 'module', 'module': 'plugins.kafka.checks.check_consumer_lag', 'function': 'run_consumer_lag'},
        ]
    },
]

def get_default_report_definition(connector, settings):
    """Returns the report structure."""
    return REPORT_SECTIONS
