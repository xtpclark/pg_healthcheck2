# -*- coding: utf-8 -*-
# High Memory Usage Queries Check for ClickHouse
# This module checks for queries consuming high memory in ClickHouse.

from plugins.clickhouse.utils.qrylib import high_memory_queries as queries


def get_weight():
    return 8


def run_high_memory_queries_check(connector, settings):
    adoc_content = []
    structured_data = {
        'high_memory_queries': {
            'status': 'unknown',
            'data': []
        }
    }

    adoc_content.append('=== High Memory Usage Queries Check')
    adoc_content.append('This check identifies queries in ClickHouse that are consuming high amounts of memory.')

    adoc_content.append('==== Query Analysis')
    try:
        query = queries.GET_HIGH_MEMORY_QUERIES
        formatted, raw = connector.execute_query(query, return_raw=True)
        if raw and len(raw) > 0:
            adoc_content.append('The following queries are consuming high memory:')
            adoc_content.append(formatted)
            structured_data['high_memory_queries']['status'] = 'success'
            structured_data['high_memory_queries']['data'] = raw
        else:
            adoc_content.append('No queries with high memory usage were found.')
            structured_data['high_memory_queries']['status'] = 'success'
            structured_data['high_memory_queries']['data'] = []
    except Exception as e:
        error_msg = f'Error retrieving high memory queries: {str(e)}'
        adoc_content.append(error_msg)
        structured_data['high_memory_queries']['status'] = 'error'
        structured_data['high_memory_queries']['details'] = error_msg

    adoc_content.append('==== Recommendations')
    if structured_data['high_memory_queries']['status'] == 'success' and len(structured_data['high_memory_queries']['data']) > 0:
        adoc_content.append('[WARNING]')
        adoc_content.append('====')
        adoc_content.append('High memory usage by queries can lead to performance degradation or crashes.')
        adoc_content.append('Consider optimizing these queries or increasing memory limits if necessary.')
    else:
        adoc_content.append('[NOTE]')
        adoc_content.append('====')
        adoc_content.append('No immediate action required as no high memory usage queries were detected.')

    return '\n'.join(adoc_content), structured_data
