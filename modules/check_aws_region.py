import boto3
from datetime import datetime, timedelta
import re # For parsing endpoint

def run_check_aws_region(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Provides AWS region-specific considerations and, if configured for Aurora/RDS,
    integrates with AWS CloudWatch to fetch key instance metrics.
    """
    adoc_content = ["=== AWS Region and Cloud Metrics", "Provides notes on AWS region-specific considerations and fetches key cloud metrics for RDS/Aurora."]
    structured_data = {} # Dictionary to hold structured findings for this module
    
    if settings['show_qry'] == 'true':
        adoc_content.append("AWS region checks and metric fetching require AWS API calls (boto3).")
        adoc_content.append("----")

    # General notes on region considerations
    # Corrected condition for is_aurora: check if it's explicitly True (boolean)
    if settings.get('is_aurora', False) == True: # Check if is_aurora is explicitly True
        adoc_content.append("[NOTE]\n====\n"
                           "For AWS RDS Aurora, region choice impacts latency, cost, and service availability. "
                           "Ensure your application is deployed in the same region or a nearby region to minimize latency. "
                           "Consider Aurora Global Database for cross-region disaster recovery. "
                           "Always verify the availability of specific instance types and Aurora features in your chosen region.\n"
                           "====\n")
        structured_data["region_considerations"] = {"status": "success", "note": "General AWS region considerations provided for Aurora."}
    else:
        adoc_content.append("[NOTE]\n====\n"
                           "This section provides general considerations for AWS regions. "
                           "For self-hosted instances on EC2, region choice impacts network latency, data transfer costs, and local service availability. "
                           "For other cloud providers (Azure, GCP), similar region-specific factors apply.\n"
                           "====\n")
        structured_data["region_considerations"] = {"status": "success", "note": "General AWS region considerations provided."}
    
    # --- AWS Cloud Metrics Integration ---
    # Corrected condition for is_aurora: check if it's explicitly True (boolean)
    if settings.get('is_aurora', False) == True: # Only attempt if configured as Aurora
        adoc_content.append("\n=== AWS CloudWatch Metrics (Aurora/RDS)\n")
        
        # Get AWS credentials from settings (assuming they are passed or will be loaded by boto3 env/config)
        # For this to work, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
        # should be set as environment variables or in ~/.aws/credentials
        
        # Extract region from endpoint (e.g., "my-instance.xxxx.us-east-1.rds.amazonaws.com")
        db_host = settings.get('host', '')
        match = re.search(r'\.([a-z0-9-]+)\.rds\.amazonaws\.com', db_host)
        aws_region = match.group(1) if match else None

        # Attempt to get DB instance/cluster identifier
        # This is a simplification; a more robust way would be to query RDS API for instance details
        # based on the endpoint, or require the user to provide the DB instance/cluster ID in config.
        db_identifier = db_host.split('.')[0] # e.g., "my-instance" from "my-instance.xxxx..."

        if not aws_region or not db_identifier:
            error_msg = f"Could not determine AWS region or DB identifier from host: {db_host}. Please ensure 'host' is a valid RDS/Aurora endpoint."
            adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
            structured_data["cloud_metrics"] = {"status": "error", "details": error_msg}
            # Return current content and data
            return "\n".join(adoc_content), structured_data

        try:
            cloudwatch = boto3.client('cloudwatch', region_name=aws_region)
            
            # Define metrics to fetch
            metrics_to_fetch = [
                {'Namespace': 'AWS/RDS', 'MetricName': 'CPUUtilization', 'Statistic': 'Average', 'Unit': 'Percent'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'FreeableMemory', 'Statistic': 'Average', 'Unit': 'Bytes'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'DatabaseConnections', 'Statistic': 'Average', 'Unit': 'Count'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'ReadIOPS', 'Statistic': 'Average', 'Unit': 'Count'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'WriteIOPS', 'Statistic': 'Average', 'Unit': 'Count'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'ReadLatency', 'Statistic': 'Average', 'Unit': 'Milliseconds'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'WriteLatency', 'Statistic': 'Average', 'Unit': 'Milliseconds'},
                {'Namespace': 'AWS/RDS', 'MetricName': 'DiskQueueDepth', 'Statistic': 'Average', 'Unit': 'Count'},
            ]

            # For Aurora, also try to get AuroraReplicaLag
            if "aurora" in db_host.lower():
                metrics_to_fetch.append({'Namespace': 'AWS/RDS', 'MetricName': 'AuroraReplicaLag', 'Statistic': 'Average', 'Unit': 'Milliseconds'})
            
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24) # Last 24 hours
            period = 3600 # 1 hour average (3600 seconds)

            fetched_metrics = {}
            for metric_info in metrics_to_fetch:
                try:
                    response = cloudwatch.get_metric_statistics(
                        Namespace=metric_info['Namespace'],
                        MetricName=metric_info['MetricName'],
                        Dimensions=[
                            {'Name': 'DBInstanceIdentifier', 'Value': db_identifier}
                            # For Aurora clusters, you might need 'DBClusterIdentifier'
                            # This is a simplification and might need refinement based on actual RDS/Aurora setup
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=period,
                        Statistics=[metric_info['Statistic']],
                        Unit=metric_info['Unit']
                    )
                    
                    if response['Datapoints']:
                        # Get the latest data point (or average over the period)
                        # We're taking the average over the entire period for simplicity
                        value = response['Datapoints'][0]['Average'] if 'Average' in response['Datapoints'][0] else response['Datapoints'][0].get(metric_info['Statistic'])
                        fetched_metrics[metric_info['MetricName']] = {
                            "value": value,
                            "unit": metric_info['Unit'],
                            "statistic": metric_info['Statistic'],
                            "timestamp": response['Datapoints'][0]['Timestamp'].isoformat()
                        }
                    else:
                        fetched_metrics[metric_info['MetricName']] = {"value": "N/A", "note": "No data points found."}
                except Exception as e:
                    fetched_metrics[metric_info['MetricName']] = {"value": "Error", "note": str(e)}
                    print(f"Warning: Could not fetch metric {metric_info['MetricName']}: {e}")

            if fetched_metrics:
                # Format for AsciiDoc
                adoc_content.append("CloudWatch Metrics Summary (Last 24 hours, hourly average):\n")
                adoc_table = ['|===', '|Metric|Value|Unit']
                for metric_name, data in fetched_metrics.items():
                    value_str = f"{data['value']:.2f}" if isinstance(data['value'], (float, Decimal)) else str(data['value'])
                    adoc_table.append(f"|{metric_name}|{value_str}|{data['unit'] if 'unit' in data else 'N/A'}")
                adoc_table.append('|===')
                adoc_content.append('\n'.join(adoc_table))
                structured_data["cloud_metrics"] = {"status": "success", "data": fetched_metrics}
            else:
                adoc_content.append("[NOTE]\n====\nNo CloudWatch metrics could be fetched. Ensure AWS credentials are configured and the DB instance identifier is correct.\n====\n")
                structured_data["cloud_metrics"] = {"status": "warning", "note": "No CloudWatch metrics fetched."}

        except Exception as e:
            error_msg = f"Failed to connect to AWS or fetch CloudWatch metrics: {e}"
            adoc_content.append(f"[ERROR]\n====\n{error_msg}\n====\n")
            structured_data["cloud_metrics"] = {"status": "error", "details": error_msg}
            print(f"Error fetching AWS CloudWatch metrics: {e}")
    else:
        adoc_content.append("\n[NOTE]\n====\nAWS CloudWatch metrics fetching skipped as 'is_aurora' is not set to 'true' in config.yaml.\n====\n")
        structured_data["cloud_metrics"] = {"status": "skipped", "note": "Cloud metrics fetching skipped (not Aurora/RDS)."}
    
    # Return both formatted AsciiDoc content and structured data
    return "\n".join(adoc_content), structured_data
