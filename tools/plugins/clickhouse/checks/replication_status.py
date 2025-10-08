def get_weight():
    return 8

def run_replication_status(connector, settings):
    adoc_content = []
    structured_data = {
        'replication_status': {'status': 'unknown', 'data': {}},
        'queue_backlog': {'status': 'unknown', 'data': {}}
    }

    adoc_content.append('=== Replication Status and Queue Backlog')
    adoc_content.append('This check verifies the replication status and identifies any delays or failures in replicated tables using system.replicas.')

    # Check replication status
    adoc_content.append('==== Replication Status')
    try:
        from plugins.clickhouse.utils.qrylib import replication_status_query
        formatted, raw = connector.execute_query(replication_status_query, return_raw=True)
        if raw:
            structured_data['replication_status']['status'] = 'success'
            structured_data['replication_status']['data'] = raw
            adoc_content.append(formatted if formatted else 'No replication issues detected.')
        else:
            structured_data['replication_status']['status'] = 'error'
            structured_data['replication_status']['data'] = {'error': 'No data returned'}
            adoc_content.append('No replication status data available.')
    except Exception as e:
        structured_data['replication_status']['status'] = 'error'
        structured_data['replication_status']['data'] = {'error': str(e)}
        adoc_content.append(f'Error retrieving replication status: {str(e)}')
        adoc_content.append('[WARNING]')
        adoc_content.append('====')
        adoc_content.append('Unable to verify replication status. This may indicate a connectivity or permissions issue.')

    # Check queue backlog
    adoc_content.append('==== Queue Backlog')
    try:
        from plugins.clickhouse.utils.qrylib import queue_backlog_query
        formatted, raw = connector.execute_query(queue_backlog_query, return_raw=True)
        if raw:
            structured_data['queue_backlog']['status'] = 'success'
            structured_data['queue_backlog']['data'] = raw
            adoc_content.append(formatted if formatted else 'No backlog detected in replication queues.')
            if any(row.get('queue_size', 0) > 0 for row in raw):
                adoc_content.append('[WARNING]')
                adoc_content.append('====')
                adoc_content.append('Replication queue backlog detected. Delays in data replication may impact data consistency across replicas.')
        else:
            structured_data['queue_backlog']['status'] = 'error'
            structured_data['queue_backlog']['data'] = {'error': 'No data returned'}
            adoc_content.append('No queue backlog data available.')
    except Exception as e:
        structured_data['queue_backlog']['status'] = 'error'
        structured_data['queue_backlog']['data'] = {'error': str(e)}
        adoc_content.append(f'Error retrieving queue backlog: {str(e)}')
        adoc_content.append('[WARNING]')
        adoc_content.append('====')
        adoc_content.append('Unable to verify queue backlog. This may indicate a connectivity or permissions issue.')

    adoc_content.append('==== Recommendations')
    adoc_content.append('- Monitor replication status regularly to ensure data consistency.')
    adoc_content.append('- Investigate any backlog in replication queues to prevent delays.')
    adoc_content.append('[TIP]')
    adoc_content.append('====')
    adoc_content.append('Use ClickHouse Keeper or ZooKeeper logs for deeper insights into replication issues if problems persist.')

    return "\n".join(adoc_content), structured_data