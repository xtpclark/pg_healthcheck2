import json


def get_topic_details_query(connector):
    """Returns query for topic details including replication status."""
    return json.dumps({
        "operation": "describe_topics",
        "topics": []  # Empty list for all topics
    })
