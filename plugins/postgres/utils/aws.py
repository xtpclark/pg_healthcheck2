import boto3
from datetime import datetime, timedelta

def get_instance_details(aws_region, db_identifier):
    """Fetches RDS instance details like instance class and storage.

    Uses the boto3 client to call the `describe_db_instances` API
    endpoint and retrieve key metadata for the specified database instance.

    Args:
        aws_region (str): The AWS region where the RDS instance is located
            (e.g., "us-east-1").
        db_identifier (str): The unique DBInstanceIdentifier for the RDS
            database instance.

    Returns:
        dict | None: A dictionary containing instance details if found,
        otherwise None. The dictionary includes 'instance_class' and
        'allocated_storage_gb'.
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
    """Fetches a set of specified metrics from Amazon CloudWatch.

    This function iterates through a list of desired metrics, queries the
    `GetMetricStatistics` API for each one over a defined time period,
    and returns the latest datapoint found for each. It gracefully handles
    cases where a metric may not exist for a given resource.

    Args:
        aws_region (str): The AWS region to query (e.g., "us-east-1").
        dimensions (list[dict]): The dimensions to filter the metrics, e.g.,
            `[{'Name': 'DBInstanceIdentifier', 'Value': 'my-db-instance'}]`.
        metrics_to_fetch (list[dict]): A list of dictionaries, each defining
            a metric. Each dict must contain 'Namespace', 'MetricName',
            'Statistic', and 'Unit'.
        hours (int): The number of hours back from now to query. Defaults to 24.
        period (int): The granularity of the data points in seconds.
            Defaults to 3600 (1 hour).

    Returns:
        dict: A dictionary where keys are the MetricNames and values are objects
              containing the latest datapoint's value, unit, statistic, and
              timestamp.
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
