import json

def get_topic_config_query(connector, topic_name: str):
    """Returns JSON query for getting config of a specific topic."""
    return json.dumps({
        "operation": "topic_config",
        "topic": topic_name
    })
