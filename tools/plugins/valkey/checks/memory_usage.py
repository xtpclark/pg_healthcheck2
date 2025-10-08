def get_weight():
    return 7

def run_memory_usage(connector, settings):
    adoc_content = []
    structured_data = {
        'memory_usage': {'status': 'unknown', 'data': {}},
        'fragmentation_ratio': {'status': 'unknown', 'data': {}}
    }

    adoc_content.append('=== Memory Usage Check')
    adoc_content.append('This check analyzes the memory usage and fragmentation ratio of the Valkey instance to identify potential memory issues.')

    # Check Memory Usage
    adoc_content.append('==== Memory Usage Analysis')
    try:
        memory_info_cmd = 'INFO MEMORY'
        formatted, raw = connector.execute_query(memory_info_cmd, return_raw=True)
        adoc_content.append(formatted)
        structured_data['memory_usage']['status'] = 'success'
        structured_data['memory_usage']['data'] = raw

        used_memory = raw.get('used_memory', 0)
        max_memory = raw.get('max_memory', 0)

        if max_memory > 0:
            usage_percentage = (used_memory / max_memory) * 100
            adoc_content.append(f'Memory Usage Percentage: {usage_percentage:.2f}%')
            if usage_percentage > 90:
                adoc_content.append('[CRITICAL]')
                adoc_content.append('====')
                adoc_content.append('Memory usage is above 90%. Consider increasing maxmemory or reducing data load.')
            elif usage_percentage > 75:
                adoc_content.append('[WARNING]')
                adoc_content.append('====')
                adoc_content.append('Memory usage is above 75%. Monitor closely for potential issues.')
        else:
            adoc_content.append('[NOTE]')
            adoc_content.append('====')
            adoc_content.append('Max memory is not configured. Unlimited memory usage may lead to system instability.')
    except Exception as e:
        adoc_content.append(f'Error retrieving memory usage: {str(e)}')
        structured_data['memory_usage']['status'] = 'error'
        structured_data['memory_usage']['details'] = str(e)

    # Check Fragmentation Ratio
    adoc_content.append('==== Fragmentation Ratio Analysis')
    try:
        fragmentation_info_cmd = 'INFO MEMORY'
        formatted, raw = connector.execute_query(fragmentation_info_cmd, return_raw=True)
        mem_fragmentation_ratio = raw.get('mem_fragmentation_ratio', 0.0)
        adoc_content.append(f'Memory Fragmentation Ratio: {mem_fragmentation_ratio:.2f}')
        structured_data['fragmentation_ratio']['status'] = 'success'
        structured_data['fragmentation_ratio']['data'] = {'ratio': mem_fragmentation_ratio}

        if mem_fragmentation_ratio > 1.5:
            adoc_content.append('[WARNING]')
            adoc_content.append('====')
            adoc_content.append('High memory fragmentation detected. Consider restarting the instance or optimizing data structures.')
        elif mem_fragmentation_ratio < 1.0:
            adoc_content.append('[TIP]')
            adoc_content.append('====')
            adoc_content.append('Fragmentation ratio below 1.0 indicates potential memory over-allocation.')
    except Exception as e:
        adoc_content.append(f'Error retrieving fragmentation ratio: {str(e)}')
        structured_data['fragmentation_ratio']['status'] = 'error'
        structured_data['fragmentation_ratio']['details'] = str(e)

    adoc_content.append('==== Recommendations')
    adoc_content.append('- Monitor memory usage regularly to avoid out-of-memory issues.')
    adoc_content.append('- Configure maxmemory if not already set to prevent unlimited memory consumption.')
    adoc_content.append('- Address high fragmentation by optimizing data or restarting the instance if necessary.')

    return '\n'.join(adoc_content), structured_data