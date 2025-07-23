import boto3
from datetime import datetime, timedelta
from decimal import Decimal

def run_aws_cloudwatch_metrics(connector, settings):
    """
    Fetches key CloudWatch metrics for RDS/Aurora instances and RDS Proxy.
    Relies on 'aws_region' and 'db_identifier' being set in the config.
    """
    adoc_content = ["=== AWS CloudWatch Metrics (Aurora/RDS)"]
    structured_data = {}

    if not settings.get('is_aurora', False):
        adoc_content.append("\n[NOTE]\n====\nThis check is for AWS RDS/Aurora environments only ('is_aurora' is false in config.yaml).\n====\n")
        structured_data["cloud_metrics"] = {"status": "skipped", "note": "Not an RDS/Aurora environment."}
        return "\n".join(adoc_content), structured_data

    # --- Get AWS settings from config ---
    aws_region = settings.get('aws_region')
    db_identifier = settings.get('db_identifier')
    rds_proxy_name = settings.get('rds_proxy_name')

    if not aws_region or not db_identifier:
        error_msg = "Could not find 'aws_region' or 'db_identifier' in config.yaml. These are required for CloudWatch metrics."
        adoc_content.append(f"\n[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["cloud_metrics"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data

    try:
        cloudwatch = boto3.client('cloudwatch', region_name=aws_region)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        period = 3600 # 1 hour average

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

        fetched_db_metrics = fetch_cloudwatch_metrics(cloudwatch, db_metrics_to_fetch, [{'Name': 'DBInstanceIdentifier', 'Value': db_identifier}], start_time, end_time, period)

        if fetched_db_metrics:
            adoc_content.append("\nRDS Instance Metrics Summary (Last 24 hours, hourly average):\n")
            adoc_content.append(format_metrics_for_adoc(fetched_db_metrics))
            structured_data["rds_cloud_metrics"] = {"status": "success", "data": fetched_db_metrics}
        else:
            adoc_content.append("\n[NOTE]\n====\nNo RDS CloudWatch metrics could be fetched. Verify permissions and the DB identifier.\n====\n")

        # --- Fetch RDS Proxy Metrics (if configured) ---
        if rds_proxy_name:
            adoc_content.append("\n==== AWS RDS Proxy Metrics\n")
            proxy_metrics_to_fetch = [
                {'Namespace': 'AWS/RDS', 'MetricName': 'ClientConnections', 'Statistic': 'Average', 'Unit': 'Count'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'DatabaseConnections', 'Statistic': 'Average', 'Unit': 'Count'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'ConnectionPinning', 'Statistic': 'Average', 'Unit': 'Percent'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'QueryResponseTime', 'Statistic': 'Average', 'Unit': 'Milliseconds'},
            ]

            fetched_proxy_metrics = fetch_cloudwatch_metrics(cloudwatch, proxy_metrics_to_fetch, [{'Name': 'DBProxyName', 'Value': rds_proxy_name}], start_time, end_time, period)

            if fetched_proxy_metrics:
                adoc_content.append("\nRDS Proxy Metrics Summary (Last 24 hours, hourly average):\n")
                adoc_content.append(format_metrics_for_adoc(fetched_proxy_metrics))
                structured_data["rds_proxy_metrics"] = {"status": "success", "data": fetched_proxy_metrics}
            else:
                adoc_content.append("\n[NOTE]\n====\nNo RDS Proxy CloudWatch metrics could be fetched. Verify the proxy name and permissions.\n====\n")

    except Exception as e:
        error_msg = f"Failed to connect to AWS or fetch CloudWatch metrics: {e}"
        adoc_content.append(f"\n[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["cloud_metrics"] = {"status": "error", "details": str(e)}

    return "\n".join(adoc_content), structured_data


def fetch_cloudwatch_metrics(cloudwatch_client, metrics_to_fetch, dimensions, start_time, end_time, period):
    """Helper function to fetch a list of metrics from CloudWatch."""
    fetched_metrics = {}
    for metric_info in metrics_to_fetch:
        try:
            response = cloudwatch_client.get_metric_statistics(
                Namespace=metric_info['Namespace'],
                MetricName=metric_info['MetricName'],
                Dimensions=dimensions,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=[metric_info['Statistic']],
            )
            
            if response['Datapoints']:
                latest_datapoint = sorted(response['Datapoints'], key=lambda x: x['Timestamp'], reverse=True)[0]
                value = latest_datapoint.get(metric_info['Statistic'])
                fetched_metrics[metric_info['MetricName']] = {
                    "value": value,
                    "unit": metric_info['Unit'],
                    "statistic": metric_info['Statistic'],
                    "timestamp": latest_datapoint['Timestamp'].isoformat()
                }
            else:
                fetched_metrics[metric_info['MetricName']] = {"value": "N/A", "note": "No data points found."}
        except Exception as e:
            fetched_metrics[metric_info['MetricName']] = {"value": "Error", "note": str(e)}
            print(f"Warning: Could not fetch metric {metric_info['MetricName']}: {e}")
    return fetched_metrics

def format_metrics_for_adoc(fetched_metrics):
    """Helper function to format fetched metrics into an AsciiDoc table."""
    adoc_table = ['[cols="3,2,2",options="header"]', '|===', '| Metric | Value | Unit']
    for metric_name, data in fetched_metrics.items():
        value_str = f"{data['value']:.2f}" if isinstance(data['value'], (float, Decimal)) else str(data['value'])
        unit_str = data.get('unit', 'N/A')
        adoc_table.append(f"| {metric_name} | `{value_str}` | {unit_str}")
    adoc_table.append('|===')
    return '\n'.join(adoc_table)
