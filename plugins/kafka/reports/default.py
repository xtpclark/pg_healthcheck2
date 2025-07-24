def get_report_definition():
    """
    Returns the default report definition for the Kafka plugin.
    """
    return [
        {
            'title': 'Kafka Cluster Overview',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.kafka.checks.broker_metadata_check',
                    'function': 'run_broker_metadata_check'
                }
            ]
        }
    ]
