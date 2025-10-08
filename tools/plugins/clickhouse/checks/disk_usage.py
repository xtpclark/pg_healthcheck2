from typing import Dict, Tuple, Any


def get_weight() -> int:
    return 7


def run_disk_usage(connector: Any, settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    adoc_content = []
    structured_data = {
        'disk_usage': {
            'status': 'pending',
            'data': {}
        }
    }

    # Header for the report
    adoc_content.append('=== Disk Usage Analysis')
    adoc_content.append('This check analyzes the disk usage and free space for each storage volume using system.disks to prevent storage-related failures.')

    # Query system.disks for disk usage information
    adoc_content.append('==== Disk Usage Statistics')
    try:
        from plugins.clickhouse.utils.qrylib.disk_usage import QUERY_DISK_USAGE
        formatted, raw = connector.execute_query(QUERY_DISK_USAGE, return_raw=True)
        if raw:
            adoc_content.append(formatted)
            structured_data['disk_usage']['status'] = 'success'
            structured_data['disk_usage']['data'] = raw
        else:
            adoc_content.append('No data returned from system.disks.')
            structured_data['disk_usage']['status'] = 'error'
            structured_data['disk_usage']['details'] = 'No data returned from system.disks.'

        # Analyze disk usage for potential issues
        adoc_content.append('==== Analysis')
        if structured_data['disk_usage']['status'] == 'success':
            high_usage_disks = []
            for disk in raw:
                free_space_percent = (disk.get('free_space', 0) / disk.get('total_space', 1)) * 100 if disk.get('total_space', 0) > 0 else 0
                if free_space_percent < 10:
                    high_usage_disks.append(disk['name'])
            if high_usage_disks:
                adoc_content.append('[WARNING]')
                adoc_content.append('====')
                adoc_content.append(f'Disks with less than 10% free space detected: {", ".join(high_usage_disks)}.')
                adoc_content.append('Consider freeing up space or expanding storage to prevent failures.')
            else:
                adoc_content.append('[NOTE]')
                adoc_content.append('====')
                adoc_content.append('All disks have sufficient free space (more than 10%).')
        else:
            adoc_content.append('Unable to perform analysis due to missing disk data.')
    except Exception as e:
        adoc_content.append(f'Error querying disk usage: {str(e)}')
        structured_data['disk_usage']['status'] = 'error'
        structured_data['disk_usage']['details'] = str(e)

    # Recommendations
    adoc_content.append('==== Recommendations')
    adoc_content.append('- Monitor disk usage regularly to avoid running out of space.')
    adoc_content.append('- Set up alerts for when free space falls below a critical threshold (e.g., 10%).')
    adoc_content.append('- Consider automating cleanup of old or unused data if applicable.')

    return '\n'.join(adoc_content), structured_data
