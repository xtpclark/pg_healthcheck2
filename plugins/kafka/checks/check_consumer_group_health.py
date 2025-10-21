from plugins.kafka.utils.qrylib.consumer_group_health_queries import get_describe_consumer_groups_query
from plugins.kafka.utils.qrylib.consumer_group_health_queries import get_all_consumer_lag_query

def get_weight():
    """Returns the importance score for this module (1-10)."""
    return 8

def run_check_consumer_group_health(connector, settings):
    """
    Analyzes consumer group health including state, member count, and lag.
    
    Collects facts from the connector and interprets them based on 
    thresholds and business logic to determine health status.
    """
    adoc_content = ["=== Consumer Group Health", ""]
    structured_data = {}
    
    try:
        # Get thresholds from settings
        critical_lag = settings.get('kafka_lag_critical', 0)
        min_members = settings.get('kafka_min_consumer_members', 2)
        
        # === STEP 1: COLLECT FACTS - Consumer Group Descriptions ===
        desc_query = get_describe_consumer_groups_query(connector)
        desc_formatted, desc_raw = connector.execute_query(desc_query, return_raw=True)
        
        # Check for errors in description data
        if "[ERROR]" in desc_formatted or (isinstance(desc_raw, dict) and 'error' in desc_raw):
            adoc_content.append(desc_formatted)
            structured_data["consumer_groups"] = {"status": "error", "data": desc_raw}
            return "\n".join(adoc_content), structured_data
        
        # Validate data structure
        if not isinstance(desc_raw, list):
            error_msg = f"[ERROR]\n====\nUnexpected consumer group data format.\n====\n"
            adoc_content.append(error_msg)
            structured_data["consumer_groups"] = {
                "status": "error", 
                "details": f"Expected list, got {type(desc_raw).__name__}"
            }
            return "\n".join(adoc_content), structured_data
        
        # === STEP 2: COLLECT FACTS - Consumer Lag ===
        lag_query = get_all_consumer_lag_query(connector)
        lag_formatted, lag_raw = connector.execute_query(lag_query, return_raw=True)
        
        # Check for errors in lag data
        if "[ERROR]" in lag_formatted or (isinstance(lag_raw, dict) and 'error' in lag_raw):
            adoc_content.append(lag_formatted)
            structured_data["consumer_groups"] = {"status": "error", "data": lag_raw}
            return "\n".join(adoc_content), structured_data
        
        # Validate lag data structure
        if not isinstance(lag_raw, dict):
            error_msg = f"[ERROR]\n====\nUnexpected lag data format.\n====\n"
            adoc_content.append(error_msg)
            structured_data["consumer_groups"] = {
                "status": "error",
                "details": f"Expected dict, got {type(lag_raw).__name__}"
            }
            return "\n".join(adoc_content), structured_data
        
        # === STEP 3: INTERPRET FACTS - Build Combined View ===
        
        # Extract facts from lag data
        group_lags_details = lag_raw.get('group_lags', [])
        groups_without_offsets = lag_raw.get('groups_without_offsets', [])
        groups_with_errors = lag_raw.get('groups_with_errors', [])
        
        # Aggregate lag by group
        group_lag_map = {}
        for item in group_lags_details:
            group_id = item.get('group_id')
            if group_id not in group_lag_map:
                group_lag_map[group_id] = {'total_lag': 0, 'details': []}
            group_lag_map[group_id]['total_lag'] += item.get('lag', 0)
            group_lag_map[group_id]['details'].append(item)
        
        # Combine group descriptions with lag data
        combined_data = []
        issues_found = False
        unstable_groups = []
        low_member_groups = []
        high_lag_groups = []
        
        for group in desc_raw:
            group_id = group.get('group_id')
            group_state = group.get('state')
            member_count = group.get('members', 0)
            
            # Determine lag status
            if group_id in groups_without_offsets:
                total_lag = 0
                has_offsets = False
            elif group_id in group_lag_map:
                total_lag = group_lag_map[group_id]['total_lag']
                has_offsets = True
            else:
                total_lag = 0
                has_offsets = False
            
            # Build combined record
            group_data = {
                'group_id': group_id,
                'state': group_state,
                'members': member_count,
                'total_lag': total_lag,
                'has_offsets': has_offsets
            }
            combined_data.append(group_data)
            
            # === INTERPRET: Apply business logic ===
            if group_state != 'Stable':
                issues_found = True
                unstable_groups.append(group_id)
            
            if member_count < min_members:
                issues_found = True
                low_member_groups.append(group_id)
            
            if has_offsets and total_lag > critical_lag:
                issues_found = True
                high_lag_groups.append(group_id)
        
        # === STEP 4: BUILD REPORT - Interpret Findings ===
        
        if not combined_data:
            adoc_content.append("[NOTE]\n====\nNo consumer groups found.\n====\n")
            structured_data["consumer_groups"] = {
                "status": "success",
                "groups_analyzed": 0,
                "data": []
            }
            return "\n".join(adoc_content), structured_data
        
        # Health summary
        if not issues_found and not groups_without_offsets:
            adoc_content.append("[NOTE]\n====\n"
                              "✅ All consumer groups are healthy.\n"
                              "====\n")
        elif not issues_found and groups_without_offsets:
            # INTERPRETATION: No offsets is normal for new consumers
            adoc_content.append("[NOTE]\n====\n"
                              "✅ All consumer groups are healthy.\n\n"
                              f"**Note:** {len(groups_without_offsets)} consumer group(s) have no committed offsets yet. "
                              "This is normal for:\n"
                              "* New consumer groups that haven't started consuming\n"
                              "* Consumers with auto-commit disabled\n"
                              "* Consumers that haven't yet committed their first offsets\n"
                              "====\n")
        else:
            # INTERPRETATION: Issues detected
            adoc_content.append("[WARNING]\n====\n"
                              "⚠️  **Action Required:** Issues detected in consumer group health.\n"
                              "====\n")
        
        # Detailed findings
        adoc_content.append("\n==== Consumer Group Summary\n")
        adoc_content.append(desc_formatted)
        
        if group_lags_details:
            adoc_content.append("\n==== Consumer Lag Details\n")
            adoc_content.append(lag_formatted)
        
        # === INTERPRETATION: Specific Issue Analysis ===
        if unstable_groups:
            adoc_content.append("\n==== Unstable Consumer Groups\n")
            adoc_content.append("[WARNING]\n====\n"
                              f"The following {len(unstable_groups)} consumer group(s) are not in 'Stable' state:\n\n")
            for group_id in unstable_groups:
                group_info = next((g for g in combined_data if g['group_id'] == group_id), {})
                adoc_content.append(f"* **{group_id}**: State = {group_info.get('state')}\n")
            adoc_content.append("\n**Impact:** Unstable states can indicate rebalancing, startup, or consumer failures.\n"
                              "====\n")
        
        if low_member_groups:
            adoc_content.append("\n==== Low Member Count\n")
            adoc_content.append("[WARNING]\n====\n"
                              f"The following {len(low_member_groups)} consumer group(s) have fewer than {min_members} member(s):\n\n")
            for group_id in low_member_groups:
                group_info = next((g for g in combined_data if g['group_id'] == group_id), {})
                adoc_content.append(f"* **{group_id}**: Members = {group_info.get('members')}\n")
            adoc_content.append("\n**Impact:** Low member count reduces redundancy and increases risk of processing delays.\n"
                              "====\n")
        
        if high_lag_groups:
            adoc_content.append("\n==== High Consumer Lag\n")
            adoc_content.append("[IMPORTANT]\n====\n"
                              f"The following {len(high_lag_groups)} consumer group(s) have lag exceeding {critical_lag:,} messages:\n\n")
            for group_id in high_lag_groups:
                group_info = next((g for g in combined_data if g['group_id'] == group_id), {})
                adoc_content.append(f"* **{group_id}**: Lag = {group_info.get('total_lag'):,} messages\n")
            adoc_content.append("\n**Impact:** High lag indicates consumers are falling behind producers. "
                              "This can lead to data loss if retention expires before messages are consumed.\n"
                              "====\n")
        
        if groups_without_offsets:
            adoc_content.append("\n==== Groups Without Committed Offsets\n")
            adoc_content.append("[NOTE]\n====\n"
                              f"The following {len(groups_without_offsets)} consumer group(s) have no committed offsets:\n\n")
            for group_id in groups_without_offsets:
                adoc_content.append(f"* {group_id}\n")
            adoc_content.append("\n**This is typically normal** for:\n"
                              "* Newly created consumer groups\n"
                              "* Groups with `enable.auto.commit=false`\n"
                              "* Groups that haven't completed their first poll cycle\n\n"
                              "**Monitor:** If these groups remain without offsets for extended periods, "
                              "verify consumers are running and able to connect.\n"
                              "====\n")
        
        if groups_with_errors:
            adoc_content.append("\n==== Groups With Query Errors\n")
            adoc_content.append("[WARNING]\n====\n"
                              f"Could not retrieve lag data for {len(groups_with_errors)} consumer group(s):\n\n")
            for error_info in groups_with_errors:
                adoc_content.append(f"* **{error_info.get('group_id')}**: {error_info.get('error')}\n")
            adoc_content.append("====\n")
        
        # === RECOMMENDATIONS ===
        if issues_found:
            adoc_content.append("\n==== Recommendations\n")
            recommendations = []
            
            if unstable_groups:
                recommendations.append("**Unstable Groups:** Investigate consumer logs for errors, "
                                     "rebalancing events, or network issues. Check if rebalancing frequency is excessive.")
            
            if low_member_groups:
                recommendations.append(f"**Low Member Count:** Consider scaling up consumer groups to at least {min_members} "
                                     "members for redundancy and better throughput.")
            
            if high_lag_groups:
                recommendations.append(f"**High Lag:** Immediately investigate consumer performance. "
                                     "Options include: scaling consumers, optimizing processing logic, "
                                     "increasing partition count, or reviewing retention policies.")
            
            adoc_content.append("[TIP]\n====\n")
            for i, rec in enumerate(recommendations, 1):
                adoc_content.append(f"{i}. {rec}\n\n")
            adoc_content.append("**Monitoring:** Set up alerts for consumer lag thresholds and group state changes.\n")
            adoc_content.append("====\n")
        
        # === STRUCTURED DATA - Pure facts for machines ===
        structured_data["consumer_groups"] = {
            "status": "success",
            "groups_analyzed": len(combined_data),
            "issues_detected": issues_found,
            "groups_with_issues": {
                "unstable": unstable_groups,
                "low_members": low_member_groups,
                "high_lag": high_lag_groups
            },
            "groups_without_offsets": groups_without_offsets,
            "groups_with_errors": groups_with_errors,
            "thresholds": {
                "critical_lag": critical_lag,
                "min_members": min_members
            },
            "data": combined_data  # Complete combined view
        }
        
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["consumer_groups"] = {
            "status": "error",
            "details": str(e)
        }
    
    return "\n".join(adoc_content), structured_data
