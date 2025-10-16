import json

def get_nodetool_compaction_query(connector):
    """
    Returns a JSON request to execute the 'nodetool compactionstats' command.
    This provides information on pending and active compactions, which is
    not available via CQL.
    """
    return json.dumps({
        "operation": "nodetool",
        "command": "compactionstats"
    })

