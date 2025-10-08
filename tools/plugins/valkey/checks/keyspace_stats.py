def get_weight():
    return 5

def run_keyspace_stats(connector, settings):
    adoc_content = []
    structured_data = {
        'keyspace_stats': {
            'status': 'unknown',
            'data': {}
        }
    }

    adoc_content.append('=== Keyspace Statistics Analysis')
    adoc_content.append('This check analyzes the distribution and size of stored data in Valkey keyspaces.')

    try:
        # Fetch keyspace statistics using INFO KEYSPACE command
        keyspace_info = connector.execute_command('INFO KEYSPACE')
        adoc_content.append('==== Keyspace Distribution')
        adoc_content.append('The following data provides an overview of keyspace statistics:')

        if keyspace_info:
            keyspace_data = {}
            for line in keyspace_info.splitlines():
                if line.startswith('db'):
                    parts = line.split(',')
                    db_name = parts[0].split(':')[0]
                    stats = {part.split('=')[0]: part.split('=')[1] for part in parts if '=' in part}
                    keyspace_data[db_name] = stats

            structured_data['keyspace_stats']['status'] = 'success'
            structured_data['keyspace_stats']['data'] = keyspace_data

            adoc_content.append('|===')
            adoc_content.append('| Database | Keys | Expires | Avg TTL')
            for db, stats in keyspace_data.items():
                adoc_content.append(f'| {db} | {stats.get("keys", "0")} | {stats.get("expires", "0")} | {stats.get("avg_ttl", "0")}')
            adoc_content.append('|===')
        else:
            structured_data['keyspace_stats']['status'] = 'error'
            structured_data['keyspace_stats']['details'] = 'No keyspace data returned.'
            adoc_content.append('No keyspace data available.')

        adoc_content.append('==== Analysis')
        if structured_data['keyspace_stats']['status'] == 'success':
            total_keys = sum(int(stats.get('keys', '0')) for stats in keyspace_data.values())
            adoc_content.append(f'Total keys across all databases: {total_keys}')
            if total_keys > 1000000:
                adoc_content.append('[WARNING]')
                adoc_content.append('====')
                adoc_content.append('A very high number of keys may indicate potential performance issues. Consider partitioning data or implementing key expiration policies.')
                adoc_content.append('====')
        
        adoc_content.append('==== Recommendations')
        adoc_content.append('- Monitor key growth over time to identify trends.')
        adoc_content.append('- Use expiration policies for temporary data to manage keyspace size.')
        adoc_content.append('[TIP]')
        adoc_content.append('====')
        adoc_content.append('Regularly review keyspace distribution to ensure optimal performance and resource usage.')
        adoc_content.append('====')

    except Exception as e:
        structured_data['keyspace_stats']['status'] = 'error'
        structured_data['keyspace_stats']['details'] = str(e)
        adoc_content.append('[CRITICAL]')
        adoc_content.append('====')
        adoc_content.append(f'Failed to retrieve keyspace statistics: {str(e)}')
        adoc_content.append('====')

    return '\n'.join(adoc_content), structured_data