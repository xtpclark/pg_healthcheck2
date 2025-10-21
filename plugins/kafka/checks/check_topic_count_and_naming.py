# Import query functions from qrylib
from plugins.kafka.utils.qrylib.list_topics_queries import get_list_topics_query

def get_weight():
    return 4

def run_check_topic_count_and_naming(connector, settings):
    """
    Analyzes topic count and naming conventions.
    
    Checks for:
    - Topic count exceeding threshold
    - Topics starting with digits
    - Topics containing spaces
    - Topics containing hyphens (best practice: use underscores)
    - Topics containing uppercase letters (best practice: lowercase only)
    """
    adoc_content = ["=== Topic Count and Naming Conventions", ""]
    structured_data = {}
    
    try:
        query = get_list_topics_query(connector)
        formatted, raw = connector.execute_query(query, return_raw=True)
        
        if "[ERROR]" in formatted:
            adoc_content.append(formatted)
            structured_data["topic_analysis"] = {"status": "error", "data": raw}
        
        elif not raw or not raw.get('topics'):
            adoc_content.append("[NOTE]\n====\nNo topics found. System is healthy.\n====\n")
            structured_data["topic_analysis"] = {
                "status": "success",
                "data": [{
                    "total_count": 0,
                    "topics": [],
                    "max_threshold": settings.get('kafka_max_topics', 100)
                }]
            }
        
        else:
            total_count = raw.get('count', 0)
            topics = raw.get('topics', [])
            max_topics = settings.get('kafka_max_topics', 100)
            
            # === UPDATED: Enhanced naming violation detection ===
            def has_naming_violation(topic_name):
                """Check if topic name violates naming conventions."""
                if not topic_name:
                    return False
                return (
                    topic_name[0].isdigit() or      # Starts with digit
                    ' ' in topic_name or             # Contains space
                    '-' in topic_name or             # Contains hyphen (prefer underscores)
                    topic_name != topic_name.lower() # Contains uppercase (prefer lowercase)
                )
            
            invalid_names = [t for t in topics if has_naming_violation(t)]
            
            # Categorize violations by type for detailed reporting
            starts_with_digit = [t for t in topics if t and t[0].isdigit()]
            contains_space = [t for t in topics if ' ' in t]
            contains_hyphen = [t for t in topics if '-' in t]
            contains_uppercase = [t for t in topics if t != t.lower()]
            
            issues_found = total_count > max_topics or invalid_names
            
            if issues_found:
                adoc_content.append("[WARNING]\n====\n**Action Required:** Issues detected in topic count or naming conventions.\n====\n")
                
                if total_count > max_topics:
                    adoc_content.append(f"**Topic Proliferation:** Current topic count {total_count} exceeds threshold {max_topics}. This may lead to performance degradation.\n\n")
                
                if invalid_names:
                    adoc_content.append(f"**Naming Violations:** {len(invalid_names)} topics do not follow naming conventions.\n\n")
                    
                    # Show violations by type
                    if starts_with_digit:
                        adoc_content.append(f"**Start with Digit** ({len(starts_with_digit)} topics):\n")
                        for topic in starts_with_digit[:5]:
                            adoc_content.append(f"  - {topic}\n")
                        if len(starts_with_digit) > 5:
                            adoc_content.append(f"  ... and {len(starts_with_digit) - 5} more\n")
                        adoc_content.append("\n")
                    
                    if contains_space:
                        adoc_content.append(f"**Contain Spaces** ({len(contains_space)} topics):\n")
                        for topic in contains_space[:5]:
                            adoc_content.append(f"  - {topic}\n")
                        if len(contains_space) > 5:
                            adoc_content.append(f"  ... and {len(contains_space) - 5} more\n")
                        adoc_content.append("\n")
                    
                    if contains_hyphen:
                        adoc_content.append(f"**Contain Hyphens** ({len(contains_hyphen)} topics):\n")
                        adoc_content.append("*Best practice: Use underscores instead of hyphens*\n\n")
                        for topic in contains_hyphen[:5]:
                            adoc_content.append(f"  - {topic}\n")
                        if len(contains_hyphen) > 5:
                            adoc_content.append(f"  ... and {len(contains_hyphen) - 5} more\n")
                        adoc_content.append("\n")
                    
                    if contains_uppercase:
                        adoc_content.append(f"**Contain Uppercase** ({len(contains_uppercase)} topics):\n")
                        adoc_content.append("*Best practice: Use lowercase for consistency*\n\n")
                        for topic in contains_uppercase[:5]:
                            adoc_content.append(f"  - {topic}\n")
                        if len(contains_uppercase) > 5:
                            adoc_content.append(f"  ... and {len(contains_uppercase) - 5} more\n")
                        adoc_content.append("\n")
                
                adoc_content.append("==== All Topics\n")
                adoc_content.append(formatted)
                
                adoc_content.append("\n==== Recommendations")
                adoc_content.append("[TIP]\n====\n")
                
                if total_count > max_topics:
                    adoc_content.append("* **Topic Proliferation:** Implement topic creation policies and review/consolidate unnecessary topics.\n")
                
                if invalid_names:
                    adoc_content.append("* **Naming Standards:** Use lowercase letters, digits, underscores, and dots only. Start with a letter.\n")
                    adoc_content.append("* **Avoid Hyphens:** Use underscores instead (hyphens can cause issues with metrics systems).\n")
                    adoc_content.append("* **Lowercase Only:** Maintain consistency and avoid case-sensitivity issues.\n")
                
                adoc_content.append("* **Monitoring:** Regularly review topic list for compliance and implement automated naming validation.\n")
                adoc_content.append("====\n")
            
            else:
                adoc_content.append("[NOTE]\n====\nNo issues detected in topic count or naming. System is healthy.\n====\n")
                adoc_content.append(formatted)
            
            # === UPDATED: Enhanced topic details with all checks ===
            topic_details = []
            for topic_name in topics:
                starts_digit = topic_name and len(topic_name) > 0 and topic_name[0].isdigit()
                has_space = ' ' in topic_name
                has_hyphen = '-' in topic_name
                has_uppercase = topic_name != topic_name.lower()
                
                topic_details.append({
                    "topic": topic_name,
                    "starts_with_digit": starts_digit,
                    "contains_space": has_space,
                    "contains_hyphen": has_hyphen,  # ← NEW
                    "contains_uppercase": has_uppercase,  # ← NEW
                    "has_violation": starts_digit or has_space or has_hyphen or has_uppercase  # ← UPDATED
                })
            
            # Store both summary and individual topic data
            structured_data["topic_analysis"] = {
                "status": "success",
                "data": [{
                    "total_count": total_count,
                    "max_threshold": max_topics,
                    "violation_count": len(invalid_names),
                    "violations_by_type": {
                        "starts_with_digit": len(starts_with_digit),
                        "contains_space": len(contains_space),
                        "contains_hyphen": len(contains_hyphen),
                        "contains_uppercase": len(contains_uppercase)
                    },
                    "exceeds_threshold": total_count > max_topics
                }]
            }
            
            structured_data["topic_details"] = {
                "status": "success",
                "data": topic_details
            }
    
    except Exception as e:
        error_msg = f"[ERROR]\n====\nCheck failed: {e}\n====\n"
        adoc_content.append(error_msg)
        structured_data["topic_analysis"] = {"status": "error", "details": str(e)}
    
    return "\n".join(adoc_content), structured_data
