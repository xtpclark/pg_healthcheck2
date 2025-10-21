from plugins.kafka.utils.qrylib.consumer_group_queries import get_describe_consumer_groups_query, get_all_consumer_lag_query
from collections import defaultdict

def get_weight():
    return 8

def run_down_members(connector, settings):
    adoc_content = ["=== Detect Down Members", ""]
    structured_data = {}
    
    try:
        # Get consumer group details
        groups_query = get_describe_consumer_groups_query(connector)
        formatted_groups, raw_groups = connector.execute_query(groups_query, return_raw=True)
        
        if "[ERROR]" in formatted_groups:
            adoc_content.append(formatted_groups)
            structured_data["down_members"] = {"status": "error", "data": []}
            return "\n".join(adoc_content), structured_data
        
        # Get all consumer lags
        lag_query = get_all_consumer_lag_query(connector)
        formatted_lag, raw_lag = connector.execute_query(lag_query, return_raw=True)
        
        if "[ERROR]" in formatted_lag:
            adoc_content.append(formatted_lag)
            structured_data["down_members"] = {"status": "error", "data": []}
            return "\n".join(adoc_content), structured_data
        
        # Aggregate total lag per group
        lag_per_group = defaultdict(int)
        for item in raw_lag.get('group_lags', []):
            lag_per_group[item['group_id']] += item.get('lag', 0)
        
        # Build data list for all groups
        group_data = [
            {
                "group_id": g['group_id'],
                "state": g['state'],
                "members": g['members'],
                "total_lag": lag_per_group.get(g['group_id'], 0)
            }
            for g in raw_groups
        ]
        
        # Find issues: groups with 0 members and positive lag
        issues = [d for d in group_data if d['members'] == 0 and d['total_lag'] > 0]
        
        structured_data["down_members"] = {"status": "success", "data": group_data}
        
        if issues:
            adoc_content.append("[WARNING]\n====\n**Action Required:** Detected " + str(len(issues)) + " consumer groups with no active members but positive lag, indicating down or stopped consumers.\n====\n")
            adoc_content.append("==== Inactive Groups with Lag")
            adoc_content.append("")
            adoc_content.append("[options=\"header\",width=\"100%\",cols=\"50%,50%\"]")
            adoc_content.append("|===")
            adoc_content.append("| Group ID | Total Lag")
            for issue in issues:
                adoc_content.append(f"| {issue['group_id']} | {issue['total_lag']}")
            adoc_content.append("|===")
            adoc_content.append("\n==== Recommendations")
            adoc_content.append("[TIP]\n====\n* **Best Practice:** Ensure consumer applications are running and properly configured to connect to the cluster.\n* **Remediation:** Restart or deploy consumers for these groups to resume processing and reduce lag.\n* **Monitoring:** Regularly check consumer group states and lags to prevent backlog accumulation.\n====\n")
        else:
            adoc_content.append("[NOTE]\n====\nNo down members detected. All consumer groups are healthy.\n====\n")
    
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["down_members"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data