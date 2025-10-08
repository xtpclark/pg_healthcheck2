# -*- coding: utf-8 -*-
# System Resource Usage Check for ClickHouse
# Monitors CPU, memory, and disk I/O usage via system.metrics to detect resource bottlenecks.

from plugins.clickhouse.utils.qrylib import system_resource_metrics


def get_weight():
    return 8  # High importance due to impact on performance


def run_system_resource_usage(connector, settings):
    adoc_content = []
    structured_data = {
        'system_resources': {
            'status': 'pending',
            'data': {}
        }
    }

    # Header for the report
    adoc_content.append('=== System Resource Usage Check')
    adoc_content.append('This check monitors CPU, memory, and disk I/O usage metrics from `system.metrics` to identify potential resource bottlenecks.')

    # Query system.metrics for resource usage data
    try:
        formatted, raw = connector.execute_query(system_resource_metrics.QUERY, return_raw=True)
        if raw:
            adoc_content.append('==== Resource Usage Metrics')
            adoc_content.append(formatted)
            structured_data['system_resources']['status'] = 'success'
            structured_data['system_resources']['data'] = raw

            # Basic analysis of key metrics
            cpu_usage = next((row['value'] for row in raw if row.get('metric') == 'CPUUsage'), None)
            memory_usage = next((row['value'] for row in raw if row.get('metric') == 'MemoryUsage'), None)
            disk_io = next((row['value'] for row in raw if row.get('metric') == 'DiskIO'), None)

            adoc_content.append('==== Analysis')
            if cpu_usage is not None and cpu_usage > 80:
                adoc_content.append('[WARNING]')
                adoc_content.append('====')
                adoc_content.append(f'High CPU usage detected: {cpu_usage}%. This may indicate a bottleneck affecting query performance.')
            if memory_usage is not None and memory_usage > 80:
                adoc_content.append('[WARNING]')
                adoc_content.append('====')
                adoc_content.append(f'High memory usage detected: {memory_usage}%. Consider allocating more memory or optimizing queries.')
            if disk_io is not None and disk_io > 80:
                adoc_content.append('[WARNING]')
                adoc_content.append('====')
                adoc_content.append(f'High disk I/O usage detected: {disk_io}%. This may slow down data operations.')
            if not any([cpu_usage > 80, memory_usage > 80, disk_io > 80]):
                adoc_content.append('No critical resource bottlenecks detected.')
        else:
            adoc_content.append('[WARNING]')
            adoc_content.append('====')
            adoc_content.append('No data returned from system.metrics. Ensure the table is accessible.')
            structured_data['system_resources']['status'] = 'error'
            structured_data['system_resources']['details'] = 'No data returned from query.'
    except Exception as e:
        adoc_content.append('[CRITICAL]')
        adoc_content.append('====')
        adoc_content.append(f'Failed to query system.metrics: {str(e)}')
        structured_data['system_resources']['status'] = 'error'
        structured_data['system_resources']['details'] = str(e)

    # Recommendations section
    adoc_content.append('==== Recommendations')
    adoc_content.append('- Monitor resource usage trends over time to identify recurring bottlenecks.')
    adoc_content.append('- If high usage persists, consider scaling hardware or optimizing workloads.')

    return "\n".join(adoc_content), structured_data
