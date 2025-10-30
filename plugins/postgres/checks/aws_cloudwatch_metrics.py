"""
AWS CloudWatch Metrics Check

Fetches CloudWatch metrics and RDS instance details for AWS RDS/Aurora environments.
Uses the connector's AWS clients and topology information for automatic discovery.
"""

from plugins.postgres.utils.aws import get_cloudwatch_metrics, get_instance_details
from plugins.common.check_helpers import CheckContentBuilder
from decimal import Decimal

def get_weight():
    """Returns the importance score for this module."""
    return 9  # Core configuration, highest importance


def run_aws_cloudwatch_metrics(connector, settings):
    """
    Fetches key CloudWatch metrics and RDS instance details using the connector's
    AWS clients and topology information.

    Args:
        connector: PostgreSQL connector with AWS support
        settings: Configuration settings

    Returns:
        tuple: (adoc_content, structured_data)
    """
    builder = CheckContentBuilder(connector.formatter)
    structured_data = {}

    builder.h3("AWS CloudWatch Metrics & Instance Details")

    # Check if this is an AWS environment using connector's auto-detection
    if not hasattr(connector, 'environment') or connector.environment not in ['aurora', 'rds']:
        env_name = getattr(connector, 'environment', 'unknown').upper() if hasattr(connector, 'environment') else 'UNKNOWN'
        builder.note(f"This check is for AWS RDS/Aurora environments only. Current environment: {env_name}")
        structured_data["aws_cloudwatch_metrics"] = {"status": "skipped", "note": "Not an RDS/Aurora environment."}
        return builder.build(), structured_data

    # Check if AWS clients are available
    if not hasattr(connector, '_cloudwatch_client') or not connector._cloudwatch_client:
        builder.critical("CloudWatch client not initialized. boto3 may not be installed or AWS credentials not configured.")
        structured_data["aws_cloudwatch_metrics"] = {"status": "error", "details": "CloudWatch client not initialized"}
        return builder.build(), structured_data

    aws_region = connector._aws_region

    # Get instance identifiers from connector's cluster topology
    instance_identifiers = []
    for node in connector.cluster_topology:
        if node.get('endpoint_type') == 'instance' and node.get('instance_id'):
            instance_identifiers.append({
                'id': node['instance_id'],
                'role': node['role'],
                'host': node['host']
            })

    # Fallback to settings if topology doesn't have instance IDs
    if not instance_identifiers and settings.get('db_identifier'):
        instance_identifiers.append({
            'id': settings['db_identifier'],
            'role': 'primary',
            'host': settings.get('host', 'N/A')
        })

    if not instance_identifiers:
        builder.critical("Could not determine DB instance identifier from cluster topology or configuration.")
        structured_data["aws_cloudwatch_metrics"] = {"status": "error", "details": "No instance identifiers found"}
        return builder.build(), structured_data

    all_normalized_metrics = []
    all_instance_details = []

    try:
        # Define metrics to fetch for each instance
        db_metrics_to_fetch = [
            {'Namespace': 'AWS/RDS', 'MetricName': 'CPUUtilization', 'Statistic': 'Average', 'Unit': 'Percent'},
            {'Namespace': 'AWS/RDS', 'MetricName': 'FreeableMemory', 'Statistic': 'Average', 'Unit': 'Bytes'},
            {'Namespace': 'AWS/RDS', 'MetricName': 'DatabaseConnections', 'Statistic': 'Average', 'Unit': 'Count'},
            {'Namespace': 'AWS/RDS', 'MetricName': 'ReadIOPS', 'Statistic': 'Average', 'Unit': 'Count/Second'},
            {'Namespace': 'AWS/RDS', 'MetricName': 'WriteIOPS', 'Statistic': 'Average', 'Unit': 'Count/Second'},
            {'Namespace': 'AWS/RDS', 'MetricName': 'ReadLatency', 'Statistic': 'Average', 'Unit': 'Milliseconds'},
            {'Namespace': 'AWS/RDS', 'MetricName': 'WriteLatency', 'Statistic': 'Average', 'Unit': 'Milliseconds'},
            {'Namespace': 'AWS/RDS', 'MetricName': 'AuroraReplicaLag', 'Statistic': 'Average', 'Unit': 'Milliseconds'},
        ]

        # Iterate through all instances in the topology
        for instance_info in instance_identifiers:
            db_identifier = instance_info['id']
            instance_role = instance_info['role']

            builder.h4(f"Instance: {db_identifier} ({instance_role.title()})")

            # --- Fetch Instance Details ---
            instance_details = get_instance_details(aws_region, db_identifier)
            if instance_details:
                instance_details['instance_id'] = db_identifier
                instance_details['role'] = instance_role
                all_instance_details.append(instance_details)

                # Display instance details
                detail_lines = [
                    f"- **Instance Class**: {instance_details.get('instance_class')}",
                    f"- **Allocated Storage**: {instance_details.get('allocated_storage_gb')} GB"
                ]
                builder.add_lines(detail_lines)

            # --- Fetch CloudWatch Metrics for this Instance ---
            db_dimensions = [{'Name': 'DBInstanceIdentifier', 'Value': db_identifier}]
            fetched_db_metrics = get_cloudwatch_metrics(aws_region, db_dimensions, db_metrics_to_fetch)
            normalized_db_metrics = _transform_aws_metrics(fetched_db_metrics)

            if normalized_db_metrics:
                # Add role context to metrics
                for metric in normalized_db_metrics:
                    metric['instance_id'] = db_identifier
                    metric['role'] = instance_role
                all_normalized_metrics.extend(normalized_db_metrics)

                # Format metrics for display
                builder.add("\n**Metrics (Last 24 hours, hourly average)**\n")
                metric_table = _format_metrics_table(normalized_db_metrics)
                builder.table(metric_table)
            else:
                builder.note("No CloudWatch metrics could be fetched for this instance. Verify permissions.")

        # Store instance details in structured data
        if all_instance_details:
            structured_data["instance_details"] = {"status": "success", "data": all_instance_details}

        # --- Fetch RDS Proxy Metrics (if configured) ---
        rds_proxy_name = settings.get('rds_proxy_name')
        if rds_proxy_name:
            builder.h4(f"RDS Proxy: {rds_proxy_name}")

            proxy_metrics_to_fetch = [
                {'Namespace': 'AWS/RDS', 'MetricName': 'ClientConnections', 'Statistic': 'Average', 'Unit': 'Count'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'DatabaseConnections', 'Statistic': 'Average', 'Unit': 'Count'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'ConnectionPinning', 'Statistic': 'Average', 'Unit': 'Percent'},
            ]
            proxy_dimensions = [{'Name': 'DBProxyName', 'Value': rds_proxy_name}]
            fetched_proxy_metrics = get_cloudwatch_metrics(aws_region, proxy_dimensions, proxy_metrics_to_fetch)
            normalized_proxy_metrics = _transform_aws_metrics(fetched_proxy_metrics)

            if normalized_proxy_metrics:
                all_normalized_metrics.extend(normalized_proxy_metrics)
                builder.add("\n**Proxy Metrics (Last 24 hours, hourly average)**\n")
                proxy_table = _format_metrics_table(normalized_proxy_metrics)
                builder.table(proxy_table)
            else:
                builder.note("No RDS Proxy CloudWatch metrics could be fetched. Verify the proxy name and permissions.")

        # --- Final Structured Data ---
        structured_data["cloud_metrics"] = {"status": "success", "data": all_normalized_metrics}

    except Exception as e:
        builder.critical(f"Failed to connect to AWS or fetch data: {e}")
        structured_data["aws_cloudwatch_metrics"] = {"status": "error", "details": str(e)}

    return builder.build(), structured_data


def _transform_aws_metrics(fetched_metrics):
    """
    Transforms the dictionary of dictionaries from AWS into a simple list of metric dictionaries.

    Args:
        fetched_metrics: Dictionary of CloudWatch metrics from get_cloudwatch_metrics()

    Returns:
        list: List of metric dictionaries with only valid numerical values
    """
    metric_list = []
    for metric_name, data in fetched_metrics.items():
        # Only include metrics that have a valid numerical value to process
        if isinstance(data.get('value'), (int, float, Decimal)):
            metric_list.append({
                'metric_name': metric_name,
                'value': data.get('value'),
                'unit': data.get('unit'),
                'statistic': data.get('statistic'),
                'note': data.get('note')
            })
    return metric_list


def _format_metrics_table(metric_list):
    """
    Format metrics list for CheckContentBuilder's table() method.

    Args:
        metric_list: List of metric dictionaries

    Returns:
        list: List of dictionaries suitable for table formatting
    """
    table_data = []
    for metric in metric_list:
        value_str = f"{metric.get('value'):.2f}" if isinstance(metric.get('value'), (float, Decimal)) else str(metric.get('value'))
        table_data.append({
            'Metric': metric.get('metric_name', 'N/A'),
            'Value': value_str,
            'Unit': metric.get('unit', 'N/A')
        })
    return table_data
