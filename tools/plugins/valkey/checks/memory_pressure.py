def get_weight():
    return 7

def run_memory_pressure_check(connector, settings):
    adoc_content = []
    structured_data = {
        'memory_pressure': {
            'status': 'unknown',
            'data': {}
        }
    }

    adoc_content.append('=== Memory Pressure and Evicted Keys Check')
    adoc_content.append('This check evaluates indicators of memory pressure in Valkey, including evicted keys and memory usage metrics.')

    try:
        # Fetch memory information using INFO MEMORY command
        adoc_content.append('==== Memory Usage Analysis')
        memory_info = connector.execute_command('INFO MEMORY')
        structured_data['memory_pressure']['data']['memory_info'] = memory_info
        structured_data['memory_pressure']['status'] = 'success'

        # Extract relevant metrics
        used_memory = int(memory_info.get('used_memory', 0))
        max_memory = int(memory_info.get('maxmemory', 0))
        evicted_keys = int(memory_info.get('evicted_keys', 0))

        adoc_content.append(f'- **Used Memory**: {used_memory:,} bytes')
        adoc_content.append(f'- **Max Memory**: {max_memory:,} bytes')
        adoc_content.append(f'- **Evicted Keys**: {evicted_keys:,}')

        if max_memory > 0:
            usage_percent = (used_memory / max_memory) * 100
            adoc_content.append(f'- **Memory Usage Percentage**: {usage_percent:.2f}%')
            structured_data['memory_pressure']['data']['usage_percent'] = usage_percent

            if usage_percent > 90:
                adoc_content.append('[CRITICAL]')
                adoc_content.append('====')
                adoc_content.append('Memory usage is critically high (above 90%). Consider increasing maxmemory or reducing data load.')
            elif usage_percent > 75:
                adoc_content.append('[WARNING]')
                adoc_content.append('====')
                adoc_content.append('Memory usage is high (above 75%). Monitor closely for potential pressure.')

        if evicted_keys > 0:
            adoc_content.append('[WARNING]')
            adoc_content.append('====')
            adoc_content.append(f'A total of {evicted_keys:,} keys have been evicted due to memory constraints. This indicates memory pressure.')
            structured_data['memory_pressure']['data']['eviction_detected'] = True
        else:
            adoc_content.append('[NOTE]')
            adoc_content.append('====')
            adoc_content.append('No keys have been evicted. Memory pressure from evictions is not currently a concern.')
            structured_data['memory_pressure']['data']['eviction_detected'] = False

    except Exception as e:
        adoc_content.append('[CRITICAL]')
        adoc_content.append('====')
        adoc_content.append(f'Failed to retrieve memory information: {str(e)}')
        structured_data['memory_pressure']['status'] = 'error'
        structured_data['memory_pressure']['details'] = str(e)

    adoc_content.append('==== Recommendations')
    adoc_content.append('- Monitor memory usage trends over time to anticipate pressure.')
    adoc_content.append('- If evictions are occurring, consider increasing the maxmemory limit or optimizing data storage.')
    adoc_content.append('- Review the eviction policy (maxmemory-policy) to ensure it aligns with your workload.')

    return '\n'.join(adoc_content), structured_data