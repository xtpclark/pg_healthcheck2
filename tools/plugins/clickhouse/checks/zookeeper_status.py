# -*- coding: utf-8 -*-
# Copyright (c) 2023-2024, HealthCheck2 Team
# License: See LICENSE file

"""
ClickHouse ZooKeeper Connection Status Check

This module checks the ZooKeeper connection status and health for ClickHouse
instances using replication. It queries the `system.zookeeper` table to assess
coordination health.
"""

from plugins.clickhouse.utils.qrylib import zookeeper_status as queries


def get_weight():
    """
    Returns the weight of this check.
    Higher weights are prioritized in reporting.
    """
    return 7


def run_zookeeper_status_check(connector, settings):
    """
    Check ZooKeeper connection status and health for ClickHouse replication.

    Args:
        connector: ClickHouse connector object for querying the database.
        settings: Dictionary of configuration settings (unused in this check).

    Returns:
        tuple: (AsciiDoc formatted string, structured data dictionary)
    """
    adoc_content = []
    structured_data = {
        'zookeeper_status': {
            'status': 'unknown',
            'data': {}
        }
    }

    # Header for the report
    adoc_content.append('=== ZooKeeper Connection Status Check')

    # Check ZooKeeper connection status via system.zookeeper
    adoc_content.append('==== Connection Status')
    try:
        formatted, raw = connector.execute_query(queries.QUERY_ZOOKEEPER_STATUS, return_raw=True)
        if raw and len(raw) > 0:
            adoc_content.append(formatted)
            structured_data['zookeeper_status']['status'] = 'success'
            structured_data['zookeeper_status']['data'] = raw
        else:
            adoc_content.append('No ZooKeeper connection data found. Is replication enabled?')
            structured_data['zookeeper_status']['status'] = 'warning'
            structured_data['zookeeper_status']['data'] = {'note': 'No data returned'}
            adoc_content.append('[WARNING]\n====')
            adoc_content.append('ZooKeeper data is unavailable. If replication is enabled, this could indicate a problem with ZooKeeper connectivity.')
    except Exception as e:
        adoc_content.append(f'Error querying ZooKeeper status: {str(e)}')
        structured_data['zookeeper_status']['status'] = 'error'
        structured_data['zookeeper_status']['details'] = {'error': str(e)}
        adoc_content.append('[CRITICAL]\n====')
        adoc_content.append('Failed to retrieve ZooKeeper status. This may impact replication coordination.')

    # Recommendations section
    adoc_content.append('==== Recommendations')
    if structured_data['zookeeper_status']['status'] == 'success':
        adoc_content.append('[NOTE]\n====')
        adoc_content.append('ZooKeeper connections appear to be active. Monitor periodically to ensure stability.')
    else:
        adoc_content.append('[TIP]\n====')
        adoc_content.append('- Verify that ZooKeeper services are running and accessible from this ClickHouse instance.')
        adoc_content.append('- Check ClickHouse configuration for correct ZooKeeper endpoints if replication is enabled.')

    return "\n".join(adoc_content), structured_data
