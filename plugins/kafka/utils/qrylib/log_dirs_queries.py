import json


def get_log_dirs_query(connector):
    """
    Returns JSON query for describing log directories on all brokers.
    """
    return json.dumps({
        "operation": "describe_log_dirs",
        "broker_ids": []  # Empty list for all brokers
    })