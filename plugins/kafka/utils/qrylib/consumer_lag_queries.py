import json

def get_all_consumer_lag_query(connector):
    """Returns query for consumer lag across all groups."""
    return json.dumps({
        "operation": "consumer_lag",
        "group_id": "*"
    })