import json


def get_cluster_metadata_query(connector):
    """Returns JSON query for cluster metadata including active brokers."""
    return json.dumps({
        "operation": "cluster_metadata"
    })