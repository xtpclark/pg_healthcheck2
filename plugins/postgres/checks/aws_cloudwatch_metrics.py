from plugins.postgres.utils.aws import get_cloudwatch_metrics
from decimal import Decimal

def format_metrics_for_adoc(fetched_metrics):
    """Helper function to format fetched metrics into an AsciiDoc table."""
    adoc_table = ['[cols="3,2,2",options="header"]', '|===', '| Metric | Value | Unit']
    for metric_name, data in fetched_metrics.items():
        value_str = f"{data['value']:.2f}" if isinstance(data['value'], (float, Decimal)) else str(data['value'])
        unit_str = data.get('unit', 'N/A')
        adoc_table.append(f"| {metric_name} | `{value_str}` | {unit_str}")
    adoc_table.append('|===')
    return '\n'.join(adoc_table)

def run_aws_cloudwatch_metrics(connector, settings):
    """
    Fetches key CloudWatch metrics for RDS/Aurora instances and RDS Proxy.
    """
    adoc_content = ["=== AWS CloudWatch Metrics (Aurora/RDS)"]
    structured_data = {}

    if not settings.get('is_aurora'):
        adoc_content.append("\n[NOTE]\n====\nThis check is for AWS RDS/Aurora environments only ('is_aurora' is false in config.yaml).\n====\n")
        structured_data["cloud_metrics_summary"] = {"status": "skipped", "note": "Not an RDS/Aurora environment."}
        return "\n".join(adoc_content), structured_data

    aws_region = settings.get('aws_region')
    db_identifier = settings.get('db_identifier')
    rds_proxy_name = settings.get('rds_proxy_name')

    if not aws_region or not db_identifier:
        error_msg = "Could not find 'aws_region' or 'db_identifier' in config.yaml for CloudWatch metrics."
        adoc_content.append(f"\n[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["cloud_metrics_summary"] = {"status": "error", "details": error_msg}
        return "\n".join(adoc_content), structured_data

    try:
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

        if fetched_db_metrics:
            adoc_content.append("\n==== RDS Instance Metrics (Last 24 hours, hourly average)")
            adoc_content.append(format_metrics_for_adoc(fetched_db_metrics))
            structured_data["rds_instance_metrics"] = {"status": "success", "data": fetched_db_metrics}

            # Create a high-level summary for the AI
            summary_data = {
                "cpu_utilization_percent": fetched_db_metrics.get("CPUUtilization", {}).get("value"),
                "freeable_memory_bytes": fetched_db_metrics.get("FreeableMemory", {}).get("value"),
                "database_connections": fetched_db_metrics.get("DatabaseConnections", {}).get("value"),
                "replica_lag_ms": fetched_db_metrics.get("AuroraReplicaLag", {}).get("value")
            }
            structured_data["cloud_metrics_summary"] = {"status": "success", "data": summary_data}
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

            if fetched_proxy_metrics:
                adoc_content.append("\n==== AWS RDS Proxy Metrics (Last 24 hours, hourly average)")
                adoc_content.append(format_metrics_for_adoc(fetched_proxy_metrics))
                structured_data["rds_proxy_metrics"] = {"status": "success", "data": fetched_proxy_metrics}
            else:
                adoc_content.append("\n[NOTE]\n====\nNo RDS Proxy CloudWatch metrics could be fetched. Verify the proxy name and permissions.\n====\n")

    except Exception as e:
        error_msg = f"Failed to connect to AWS or fetch CloudWatch metrics: {e}"
        adoc_content.append(f"\n[ERROR]\n====\n{error_msg}\n====\n")
        structured_data["cloud_metrics_summary"] = {"status": "error", "details": str(e)}

    return "\n".join(adoc_content), structured_data
