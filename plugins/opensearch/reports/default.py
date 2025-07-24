def get_report_definition():
    """
    Returns the default report definition for the OpenSearch plugin.
    """
    return [
        {
            'title': 'OpenSearch Cluster Health',
            'actions': [
                {
                    'type': 'module',
                    'module': 'plugins.opensearch.checks.cluster_health_check',
                    'function': 'run_cluster_health_check'
                }
            ]
        }
    ]
