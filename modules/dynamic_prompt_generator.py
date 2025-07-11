#!/usr/bin/env python3
"""
Dynamic Prompt Generator for PostgreSQL Health Check AI Analysis

This module analyzes structured findings and generates context-aware prompts
based on the severity and type of issues detected in the database.
"""

import json
from decimal import Decimal
from datetime import datetime, timedelta

def convert_to_json_serializable(obj):
    """Convert non-JSON-serializable objects to JSON-compatible types."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, timedelta):
        return obj.total_seconds()
    elif isinstance(obj, dict):
        return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_json_serializable(item) for item in obj]
    else:
        return obj

def analyze_metric_severity(metric_name, data, settings):
    """
    Analyze the severity of a specific metric based on its data and context.
    
    Args:
        metric_name (str): Name of the metric being analyzed
        data (dict/list): The metric data
        settings (dict): Configuration settings
        
    Returns:
        dict: Severity analysis with level, score, and reasoning
    """
    severity_levels = {
        'critical': 5,
        'high': 4, 
        'medium': 3,
        'low': 2,
        'info': 1
    }
    
    analysis = {
        'level': 'info',
        'score': 0,
        'reasoning': '',
        'recommendations': []
    }
    
    # Database connection metrics
    if 'connection' in metric_name.lower():
        if isinstance(data, list) and len(data) > 0:
            for row in data:
                if 'total_connections' in row and 'max_connections' in row:
                    try:
                        current = int(row['total_connections'])
                        max_conn = int(row['max_connections'])
                        usage_pct = (current / max_conn) * 100
                        
                        if usage_pct > 90:
                            analysis['level'] = 'critical'
                            analysis['score'] = 5
                            analysis['reasoning'] = f"Connection usage at {usage_pct:.1f}% of maximum"
                            analysis['recommendations'].append("Immediate action required: Connection pool near capacity")
                        elif usage_pct > 75:
                            analysis['level'] = 'high'
                            analysis['score'] = 4
                            analysis['reasoning'] = f"Connection usage at {usage_pct:.1f}% of maximum"
                            analysis['recommendations'].append("Monitor connection usage and consider connection pooling")
                    except (ValueError, TypeError):
                        pass
    
    # Query performance metrics
    elif 'query' in metric_name.lower() or 'statements' in metric_name.lower():
        if isinstance(data, list) and len(data) > 0:
            for row in data:
                if 'total_exec_time' in row:
                    try:
                        exec_time = float(row['total_exec_time'])
                        if exec_time > 3600000:  # > 1 hour total
                            analysis['level'] = 'critical'
                            analysis['score'] = 5
                            analysis['reasoning'] = f"Query with {exec_time/1000:.1f}s total execution time"
                            analysis['recommendations'].append("Optimize or terminate long-running queries")
                        elif exec_time > 600000:  # > 10 minutes
                            analysis['level'] = 'high'
                            analysis['score'] = 4
                            analysis['reasoning'] = f"Query with {exec_time/1000:.1f}s total execution time"
                            analysis['recommendations'].append("Investigate query performance")
                    except (ValueError, TypeError):
                        pass
    
    # Index analysis
    elif 'index' in metric_name.lower():
        if isinstance(data, list) and len(data) > 0:
            unused_count = 0
            large_count = 0
            
            for row in data:
                if 'idx_scan' in row:
                    try:
                        scans = int(row['idx_scan'])
                        if scans == 0:
                            unused_count += 1
                    except (ValueError, TypeError):
                        pass
                
                if 'pg_size_pretty' in str(row) or 'size' in row:
                    large_count += 1
            
            if unused_count > 5:
                analysis['level'] = 'high'
                analysis['score'] = 4
                analysis['reasoning'] = f"Found {unused_count} unused indexes"
                analysis['recommendations'].append("Consider dropping unused indexes to improve write performance")
                analysis['recommendations'].append("⚠️ CRITICAL: Check ALL read replicas before removing - indexes may be used on replicas!")
            elif unused_count > 0:
                analysis['level'] = 'medium'
                analysis['score'] = 3
                analysis['reasoning'] = f"Found {unused_count} unused indexes"
                analysis['recommendations'].append("Review unused indexes for potential removal")
                analysis['recommendations'].append("⚠️ IMPORTANT: Verify index usage on read replicas before removal")
    
    # Vacuum and bloat analysis
    elif 'vacuum' in metric_name.lower() or 'bloat' in metric_name.lower():
        if isinstance(data, list) and len(data) > 0:
            for row in data:
                if 'n_dead_tup' in row and 'n_live_tup' in row:
                    try:
                        dead = int(row['n_dead_tup'])
                        live = int(row['n_live_tup'])
                        if live > 0:
                            dead_ratio = dead / (dead + live)
                            if dead_ratio > 0.3:
                                analysis['level'] = 'critical'
                                analysis['score'] = 5
                                analysis['reasoning'] = f"High dead tuple ratio: {dead_ratio:.1%}"
                                analysis['recommendations'].append("Immediate VACUUM required to prevent bloat")
                            elif dead_ratio > 0.1:
                                analysis['level'] = 'high'
                                analysis['score'] = 4
                                analysis['reasoning'] = f"Elevated dead tuple ratio: {dead_ratio:.1%}"
                                analysis['recommendations'].append("Schedule VACUUM to prevent bloat")
                    except (ValueError, TypeError):
                        pass
    
    # Security analysis
    elif 'security' in metric_name.lower() or 'ssl' in metric_name.lower():
        if isinstance(data, list) and len(data) > 0:
            non_ssl_count = 0
            total_count = 0
            
            for row in data:
                if 'ssl' in row:
                    total_count += 1
                    if not row['ssl'] or row['ssl'] == 'f':
                        non_ssl_count += 1
            
            if total_count > 0:
                ssl_ratio = non_ssl_count / total_count
                if ssl_ratio > 0.5:
                    analysis['level'] = 'critical'
                    analysis['score'] = 5
                    analysis['reasoning'] = f"Only {((1-ssl_ratio)*100):.1f}% of connections use SSL"
                    analysis['recommendations'].append("Enforce SSL connections for security")
                elif ssl_ratio > 0.1:
                    analysis['level'] = 'high'
                    analysis['score'] = 4
                    analysis['reasoning'] = f"{ssl_ratio*100:.1f}% of connections don't use SSL"
                    analysis['recommendations'].append("Consider enforcing SSL for all connections")
    
    # Aurora-specific metrics
    elif 'aurora' in metric_name.lower() and settings.get('is_aurora', False):
        if isinstance(data, list) and len(data) > 0:
            for row in data:
                if 'replica_lag' in row:
                    try:
                        lag = float(row['replica_lag'])
                        if lag > 300:  # > 5 minutes
                            analysis['level'] = 'critical'
                            analysis['score'] = 5
                            analysis['reasoning'] = f"High replica lag: {lag:.1f} seconds"
                            analysis['recommendations'].append("Investigate Aurora replica lag immediately")
                        elif lag > 60:  # > 1 minute
                            analysis['level'] = 'high'
                            analysis['score'] = 4
                            analysis['reasoning'] = f"Elevated replica lag: {lag:.1f} seconds"
                            analysis['recommendations'].append("Monitor Aurora replica performance")
                    except (ValueError, TypeError):
                        pass
    
    return analysis

def generate_dynamic_prompt(all_structured_findings, settings):
    """
    Generate a dynamic, context-aware prompt based on the collected metrics.
    Instruct the AI to return its analysis as properly formatted AsciiDoc, with a well-structured layout including:
    - Headings
    - An Executive Summary section
    - Sections organized by priority/criticality
    - Advanced formatting rules for clarity and professionalism
    - SRE/DBA audience: technical language, SQL/config references, operational impact, risk, and downtime warnings
    """
    # Convert findings to JSON-serializable format
    findings_for_analysis = convert_to_json_serializable(all_structured_findings)
    
    # Analyze all metrics for severity
    metric_analyses = {}
    critical_issues = []
    high_priority_issues = []
    medium_priority_issues = []
    
    for module_name, module_findings in findings_for_analysis.items():
        if module_findings.get("status") == "success" and module_findings.get("data"):
            # Analyze each data key within the module
            for data_key, data in module_findings.get("data", {}).items():
                if isinstance(data, dict) and "data" in data:
                    # Handle nested data structure
                    analysis = analyze_metric_severity(f"{module_name}_{data_key}", data["data"], settings)
                else:
                    analysis = analyze_metric_severity(f"{module_name}_{data_key}", data, settings)
                
                metric_analyses[f"{module_name}_{data_key}"] = analysis
                
                # Categorize by severity
                if analysis['level'] == 'critical':
                    critical_issues.append({
                        'metric': f"{module_name}_{data_key}",
                        'analysis': analysis
                    })
                elif analysis['level'] == 'high':
                    high_priority_issues.append({
                        'metric': f"{module_name}_{data_key}",
                        'analysis': analysis
                    })
                elif analysis['level'] == 'medium':
                    medium_priority_issues.append({
                        'metric': f"{module_name}_{data_key}",
                        'analysis': analysis
                    })
    
    # Generate context-aware prompt
    prompt_parts = []
    prompt_parts.append("You are an expert PostgreSQL health check analyst. Your audience is SREs and DBAs.\n")
    prompt_parts.append("Use technical language, include relevant SQL commands, configuration parameters, and catalog/table references.\n")
    prompt_parts.append("Focus on operational impact, performance, reliability, and risk mitigation.\n")
    prompt_parts.append("For each recommendation, include the specific metric, table, or finding that triggered it, the expected operational impact, and any risk or urgency tags (e.g., [IMMEDIATE], [HIGH RISK]).\n")
    prompt_parts.append("If a recommendation requires downtime or a restart, clearly state this in a [CAUTION] or [IMPORTANT] block.\n")
    prompt_parts.append("If there are no issues in a section, state 'No action needed.'\n")
    prompt_parts.append("Keep the Executive Summary concise and technical.\n")
    prompt_parts.append("If possible, include summary tables or AsciiDoc code blocks for clarity.\n")
    prompt_parts.append("\nYour output MUST follow these formatting requirements:\n")
    prompt_parts.append("- Use `===` for the main section title (e.g., `=== AI-Generated Recommendations`).\n")
    prompt_parts.append("- Use `====` for major subsections (e.g., `==== Executive Summary`, `==== Critical Issues`).\n")
    prompt_parts.append("- Use `=====` for individual recommendations or grouped topics within each priority.\n")
    prompt_parts.append("- Include an 'Executive Summary' section (with a heading) summarizing the overall health, most urgent issues, and general trends.\n")
    prompt_parts.append("- Order sections by severity: Critical, High, Medium, Low, Info. If a section is empty, state 'No issues of this priority detected.'\n")
    prompt_parts.append("- For each recommendation, include:\n")
    prompt_parts.append("  * A short, actionable title\n")
    prompt_parts.append("  * A brief description (1–2 sentences) explaining the issue and why it matters\n")
    prompt_parts.append("  * Action steps (bulleted or numbered list)\n")
    prompt_parts.append("  * Relevant data (inline code or table, if applicable)\n")
    prompt_parts.append("  * References (optional: link to docs or best practices)\n")
    prompt_parts.append("- Use AsciiDoc tables for comparisons, before/after, or lists of affected objects.\n")
    prompt_parts.append("- Use bullet points for steps, warnings, or grouped findings.\n")
    prompt_parts.append("- Use [IMPORTANT], [CAUTION], [TIP], [NOTE] blocks for emphasis.\n")
    prompt_parts.append("- Use [source,sql] or [source,bash] for SQL or shell commands.\n")
    prompt_parts.append("- Optionally, add a 'Further Reading' or 'References' section at the end.\n")
    prompt_parts.append("- Include the date/time of the analysis at the top, and optionally the database version and environment.\n")
    prompt_parts.append("- Do NOT include any markdown, only AsciiDoc.\n\n")
    prompt_parts.append("Here is the structured findings data for your analysis:\n\n")
    prompt_parts.append(json.dumps(findings_for_analysis, indent=2))
    prompt_parts.append("\n\nFocus on performance, stability, and security improvements relevant to a PostgreSQL database.\nIf 'is_aurora' is true in settings, include Aurora-specific advice where relevant.\n")
    
    return {
        'prompt': ''.join(prompt_parts),
        'critical_issues': critical_issues,
        'high_priority_issues': high_priority_issues,
        'medium_priority_issues': medium_priority_issues,
        'total_issues': len(critical_issues) + len(high_priority_issues) + len(medium_priority_issues)
    }

def run_dynamic_prompt_generator(cursor, settings, execute_query, execute_pgbouncer, all_structured_findings):
    """
    Generate a dynamic prompt based on the collected metrics and their severity analysis.
    
    This function can be used as an alternative to the standard prompt generation
    in run_recommendation.py, providing more context-aware and prioritized recommendations.
    """
    adoc_content = ["Generates context-aware prompts based on metric severity analysis.\n"]
    structured_data = {}
    
    # Generate the dynamic prompt
    dynamic_analysis = generate_dynamic_prompt(all_structured_findings, settings)
    
    # Store the analysis results
    structured_data["dynamic_prompt_analysis"] = {
        "status": "success",
        "data": {
            "total_issues": dynamic_analysis['total_issues'],
            "critical_issues": len(dynamic_analysis['critical_issues']),
            "high_priority_issues": len(dynamic_analysis['high_priority_issues']),
            "medium_priority_issues": len(dynamic_analysis['medium_priority_issues']),
            "metric_analyses": dynamic_analysis['metric_analyses']
        }
    }
    
    # Add summary to the report
    adoc_content.append(f"** Dynamic Analysis Summary **\n")
    adoc_content.append(f"- Total Issues Detected: {dynamic_analysis['total_issues']}\n")
    adoc_content.append(f"- Critical Issues: {len(dynamic_analysis['critical_issues'])}\n")
    adoc_content.append(f"- High Priority Issues: {len(dynamic_analysis['high_priority_issues'])}\n")
    adoc_content.append(f"- Medium Priority Issues: {len(dynamic_analysis['medium_priority_issues'])}\n\n")
    
    if dynamic_analysis['critical_issues']:
        adoc_content.append("🚨 **CRITICAL ISSUES REQUIRING IMMEDIATE ATTENTION:**\n")
        for issue in dynamic_analysis['critical_issues']:
            adoc_content.append(f"- {issue['metric'].replace('_', ' ').title()}: {issue['analysis']['reasoning']}\n")
        adoc_content.append("\n")
    
    if dynamic_analysis['high_priority_issues']:
        adoc_content.append("⚠️ **HIGH PRIORITY ISSUES:**\n")
        for issue in dynamic_analysis['high_priority_issues']:
            adoc_content.append(f"- {issue['metric'].replace('_', ' ').title()}: {issue['analysis']['reasoning']}\n")
        adoc_content.append("\n")
    
    # Store the generated prompt for use in AI analysis
    structured_data["generated_prompt"] = dynamic_analysis['prompt']
    
    adoc_content.append("[TIP]\n====\n")
    adoc_content.append("This dynamic analysis provides context-aware prompting for AI recommendations. ")
    adoc_content.append("The prompt is tailored based on the severity and type of issues detected in your database. ")
    adoc_content.append("Critical issues are prioritized for immediate attention, while medium-priority issues are flagged for monitoring.\n")
    adoc_content.append("====\n")
    
    return "\n".join(adoc_content), structured_data 