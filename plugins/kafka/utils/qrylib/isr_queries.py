import json


def get_describe_topics_query(connector):
    """Returns JSON query for describing all topics to check ISR status."""
    return json.dumps({
        "operation": "describe_topics",
        "topics": []  # Empty list for all topics
    })