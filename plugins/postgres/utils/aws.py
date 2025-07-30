import boto3
from datetime import datetime, timedelta

def get_instance_details(aws_region, db_identifier):
    """
    Fetches RDS instance details like instance class and allocated storage.
    """
    rds = boto3.client('rds', region_name=aws_region)
    try:
        response = rds.describe_db_instances(DBInstanceIdentifier=db_identifier)
        if response['DBInstances']:
            instance = response['DBInstances'][0]
            # This can be expanded later to fetch RAM size based on instance class
            return {
                "instance_class": instance.get('DBInstanceClass'),
                "allocated_storage_gb": instance.get('AllocatedStorage')
            }
    except Exception as e:
        print(f"Warning: Could not fetch RDS instance details for {db_identifier}: {e}")
    return None

def get_cloudwatch_metrics(aws_region, dimensions, metrics_to_fetch, hours=24, period=3600):
    """
    Generic helper function to fetch a list of metrics from CloudWatch.

    Args:
        aws_region (str): The AWS region to query.
        dimensions (list): The dimensions to filter the metrics (e.g., DBInstanceIdentifier).
        metrics_to_fetch (list): A list of dictionaries, each defining a metric to fetch.
        hours (int): The number of hours back from now to query.
        period (int): The granularity of the data points in seconds.

    Returns:
        dict: A dictionary of the fetched metrics and their latest values.
    """
    cloudwatch = boto3.client('cloudwatch', region_name=aws_region)
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)
    
    fetched_metrics = {}
    for metric_info in metrics_to_fetch:
        try:
            response = cloudwatch.get_metric_statistics(
                Namespace=metric_info['Namespace'],
                MetricName=metric_info['MetricName'],
                Dimensions=dimensions,
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=[metric_info['Statistic']],
                Unit=metric_info['Unit']
            )
            
            if response['Datapoints']:
                # Get the most recent datapoint
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
            # Gracefully handle missing metrics (e.g., AuroraReplicaLag on a non-Aurora instance)
            fetched_metrics[metric_info['MetricName']] = {"value": "N/A", "note": f"Could not fetch: {e}"}
            print(f"Warning: Could not fetch CloudWatch metric {metric_info['MetricName']}: {e}")
            
    return fetched_metrics
