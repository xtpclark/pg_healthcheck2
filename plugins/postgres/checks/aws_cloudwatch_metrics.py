from plugins.postgres.utils.aws import get_cloudwatch_metrics, get_instance_details
from decimal import Decimal

def get_weight():
    """Returns the importance score for this module."""
    return 9 # Core configuration, highest importance

def format_metrics_for_adoc(metric_list):
    """Helper function to format a list of metric dictionaries into an AsciiDoc table."""
    adoc_table = ['[cols="3,2,2",options="header"]', '|===', '| Metric | Value | Unit']
    for metric in metric_list:
        metric_name = metric.get('metric_name', 'N/A')
        value_str = f"{metric.get('value'):.2f}" if isinstance(metric.get('value'), (float, Decimal)) else str(metric.get('value'))
        unit_str = metric.get('unit', 'N/A')
        adoc_table.append(f"| {metric_name} | `{value_str}` | {unit_str}")
    adoc_table.append('|===')
    return '\n'.join(adoc_table)

def _transform_aws_metrics(fetched_metrics):
    """Transforms the dictionary of dictionaries from AWS into a simple list of metric dictionaries."""
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

def run_aws_cloudwatch_metrics(connector, settings):
    """
    Fetches key CloudWatch metrics and RDS instance details, and normalizes 
    the data into a consistent list format.
    """
    adoc_content = ["=== AWS CloudWatch Metrics & Instance Details"]
    structured_data = {}

    if not settings.get('is_aurora'):
        adoc_content.append("\n[NOTE]\n====\nThis check is for AWS RDS/Aurora environments only ('is_aurora' is false in config.yaml).\n====\n")
        structured_data["aws_cloudwatch_metrics"] = {"status": "skipped", "note": "Not an RDS/Aurora environment."}
        return "\n".join(adoc_content), structured_data

    aws_region = settings.get('aws_region')
    db_identifier = settings.get('db_identifier')
    rds_proxy_name = settings.get('rds_proxy_name')

    if not aws_region or not db_identifier:
        error_msg = "Could not find 'aws_region' or 'db_identifier' in config.yaml for AWS data collection."
        adoc_content.append(f"\n[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["aws_cloudwatch_metrics"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data

    all_normalized_metrics = []

    try:
        # --- Fetch Instance Details ---
        instance_details = get_instance_details(aws_region, db_identifier)
        if instance_details:
            adoc_content.append("\n==== RDS Instance Details")
            adoc_content.append(f"- **Instance Class**: {instance_details.get('instance_class')}")
            adoc_content.append(f"- **Allocated Storage**: {instance_details.get('allocated_storage_gb')} GB")
            structured_data["instance_details"] = {"status": "success", "data": instance_details}

        # --- Fetch RDS Instance Metrics ---
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
        
        db_dimensions = [{'Name': 'DBInstanceIdentifier', 'Value': db_identifier}]
        fetched_db_metrics = get_cloudwatch_metrics(aws_region, db_dimensions, db_metrics_to_fetch)
        normalized_db_metrics = _transform_aws_metrics(fetched_db_metrics)

        if normalized_db_metrics:
            all_normalized_metrics.extend(normalized_db_metrics)
            adoc_content.append("\n==== RDS Instance Metrics (Last 24 hours, hourly average)")
            adoc_content.append(format_metrics_for_adoc(normalized_db_metrics))
        else:
            adoc_content.append("\n[NOTE]\n====\nNo RDS CloudWatch metrics could be fetched. Verify permissions and the DB identifier.\n====\n")

        # --- Fetch RDS Proxy Metrics (if configured) ---
        if rds_proxy_name:
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
                adoc_content.append("\n==== AWS RDS Proxy Metrics (Last 24 hours, hourly average)")
                adoc_content.append(format_metrics_for_adoc(normalized_proxy_metrics))
            else:
                adoc_content.append("\n[NOTE]\n====\nNo RDS Proxy CloudWatch metrics could be fetched. Verify the proxy name and permissions.\n====\n")

        # --- Final Structured Data ---
        structured_data["cloud_metrics"] = {"status": "success", "data": all_normalized_metrics}


    except Exception as e:
        error_msg = f"Failed to connect to AWS or fetch data: {e}"
        adoc_content.append(f"\n[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["aws_cloudwatch_metrics"] = {"status": "error", "details": str(e)}

    return "\n".join(adoc_content), structured_data
