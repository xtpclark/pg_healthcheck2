#!/usr/bin/env python3
"""
Dynamic Prompt Generator for PostgreSQL Health Check AI Analysis (Refactored)

This module analyzes structured findings and generates context-aware prompts
based on the severity and type of issues detected in the database.
It uses a configurable approach for metric analysis and Jinja2 for templating.
It also includes logic to manage prompt size to avoid exceeding AI model token limits.
"""

import json
from decimal import Decimal
from datetime import datetime, timedelta
import jinja2
from pathlib import Path

# --- Configuration for Metric Analysis ---
METRIC_ANALYSIS_CONFIG = {
    # --- Rule for Hot Query Workload Concentration ---
    'query_workload_concentration': {
        'metric_keywords': ['hot_query_summary'],
        'data_conditions': [{'key': 'total_queries_tracked', 'exists': True}],
        'rules': [
            {
                'expression': (
                    "data.get('total_execution_time_all_queries_ms') and data['total_execution_time_all_queries_ms'] > 0 and "
                    "(sum(q.get('total_exec_time', q.get('total_time', 0)) or 0 for q in all_structured_findings.get('hot_queries', {}).get('data', {}).get('top_hot_queries', {}).get('data', [])) / data['total_execution_time_all_queries_ms']) * 100 > 75"
                ),
                'level': 'critical',
                'score': 5,
                'reasoning': "High workload concentration detected. The top {settings['row_limit']} queries account for more than 75% of the total database execution time.",
                'recommendations': ["Focus optimization efforts on the top queries, as this will yield the most significant performance improvements."]
            }
        ]
    },
    'connection_usage': {
        'metric_keywords': ['connection'],
        'data_conditions': [{'key': 'total_connections', 'exists': True}, {'key': 'max_connections', 'exists': True}],
        'rules': [
            {'expression': "not settings.get('using_connection_pooler', False) and (int(data['total_connections']) / int(data['max_connections'])) * 100 > 90", 'level': 'critical', 'score': 5, 'reasoning': "Connection usage at {(int(data['total_connections']) / int(data['max_connections'])) * 100:.1f}% of maximum", 'recommendations': ["Immediate action required: Connection pool near capacity"]},
            {'expression': "not settings.get('using_connection_pooler', False) and (int(data['total_connections']) / int(data['max_connections'])) * 100 > 75", 'level': 'high', 'score': 4, 'reasoning': "Connection usage at {(int(data['total_connections']) / int(data['max_connections'])) * 100:.1f}% of maximum", 'recommendations': ["Monitor connection usage and consider connection pooling"]}
        ]
    },
    'long_running_queries': {
        'metric_keywords': ['query', 'statements'],
        'data_conditions': [{'key': 'total_exec_time', 'exists': True}],
        'rules': [
            {'expression': "float(data['total_exec_time']) > 3600000", 'level': 'critical', 'score': 5, 'reasoning': "Query with {float(data['total_exec_time']) / 1000:.1f}s total execution time", 'recommendations': ["Optimize or terminate long-running queries"]},
            {'expression': "float(data['total_exec_time']) > 600000", 'level': 'high', 'score': 4, 'reasoning': "Query with {float(data['total_exec_time']) / 1000:.1f}s total execution time", 'recommendations': ["Investigate query performance"]}
        ]
    },
    'unused_indexes': {
        'metric_keywords': ['index'],
        'data_conditions': [{'key': 'idx_scan', 'exists': True}],
        'rules': [
            {'expression': "int(data['idx_scan']) == 0", 'level': 'medium', 'score': 3, 'reasoning': "Found potentially unused index: {data['index_name']}", 'recommendations': ["Review index usage on all replicas before removal"]}
        ]
    },
    'vacuum_bloat': {
        'metric_keywords': ['bloated_tables'],
        'data_conditions': [{'key': 'n_dead_tup', 'exists': True}, {'key': 'n_live_tup', 'exists': True}],
        'rules': [
            {'expression': "int(data['n_live_tup']) > 0 and (int(data['n_dead_tup']) / (int(data['n_dead_tup']) + int(data['n_live_tup']))) > 0.5", 'level': 'critical', 'score': 5, 'reasoning': "Critically high dead tuple ratio in table {data.get('relname', 'N/A')}", 'recommendations': ["Immediate VACUUM required"]},
            {'expression': "int(data['n_live_tup']) > 0 and (int(data['n_dead_tup']) / (int(data['n_dead_tup']) + int(data['n_live_tup']))) > 0.2", 'level': 'high', 'score': 4, 'reasoning': "High dead tuple ratio in table {data.get('relname', 'N/A')}", 'recommendations': ["Schedule VACUUM to prevent bloat"]}
        ]
    },
    'systemic_bloat': {
        'metric_keywords': ['bloat_summary'],
        'data_conditions': [{'key': 'tables_with_high_bloat', 'exists': True}],
        'rules': [
            {'expression': "int(data['tables_with_critical_bloat']) > 5", 'level': 'critical', 'score': 5, 'reasoning': "Systemic bloat detected: {data['tables_with_critical_bloat']} tables have critical bloat levels (>50%).", 'recommendations': ["Global autovacuum settings are likely misconfigured for the workload. Review and tune immediately."]},
            {'expression': "int(data['tables_with_high_bloat']) > 10", 'level': 'high', 'score': 4, 'reasoning': "Systemic bloat detected: {data['tables_with_high_bloat']} tables have high bloat levels (>20%).", 'recommendations': ["Global autovacuum settings may need tuning. Investigate workload patterns."]}
        ]
    },
    'aws_cpu_utilization': {
        'metric_keywords': ['CPUUtilization'],
        'data_conditions': [{'key': 'value', 'exists': True}],
        'rules': [
            {'expression': "float(data['value']) > 90", 'level': 'critical', 'score': 5, 'reasoning': "CPU Utilization is critically high at {data['value']:.1f}%.", 'recommendations': ["Investigate top queries, consider scaling instance class."]},
            {'expression': "float(data['value']) > 75", 'level': 'high', 'score': 4, 'reasoning': "CPU Utilization is high at {data['value']:.1f}%.", 'recommendations': ["Monitor CPU usage and optimize resource-intensive queries."]}
        ]
    },
    'aws_free_storage': {
        'metric_keywords': ['FreeStorageSpace'],
        'data_conditions': [{'key': 'value', 'exists': True}],
        'rules': [
            {'expression': "float(data['value']) < 10 * 1024**3", 'level': 'critical', 'score': 5, 'reasoning': "Free storage space is critically low at {data['value'] / 1024**3:.2f} GB.", 'recommendations': ["Increase storage volume immediately to prevent outage."]},
            {'expression': "float(data['value']) < 25 * 1024**3", 'level': 'high', 'score': 4, 'reasoning': "Free storage space is low at {data['value'] / 1024**3:.2f} GB.", 'recommendations': ["Plan to increase storage volume soon."]}
        ]
    },
    'aws_burst_balance': {
        'metric_keywords': ['BurstBalance'],
        'data_conditions': [{'key': 'value', 'exists': True}],
        'rules': [
            {'expression': "float(data['value']) < 10", 'level': 'high', 'score': 4, 'reasoning': "Storage burst balance is low at {data['value']:.1f}%, performance may be throttled.", 'recommendations': ["Consider switching to Provisioned IOPS (io1) or gp3 storage if performance is impacted."]}
        ]
    },
    'rds_proxy_pinning': {
        'metric_keywords': ['ConnectionPinning'],
        'data_conditions': [{'key': 'value', 'exists': True}],
        'rules': [
            {'expression': "float(data['value']) > 5", 'level': 'high', 'score': 4, 'reasoning': "RDS Proxy is experiencing connection pinning ({data['value']:.1f}%), reducing pooler efficiency.", 'recommendations': ["Investigate application queries for session-level settings that cause pinning."]}
        ]
    }
}

def convert_to_json_serializable(obj):
    """Convert non-JSON-serializable objects to JSON-compatible types."""
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, datetime): return obj.isoformat()
    if isinstance(obj, timedelta): return obj.total_seconds()
    if isinstance(obj, dict): return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list): return [convert_to_json_serializable(item) for item in obj]
    return obj

def analyze_metric_severity(metric_name, data_row, settings, all_findings):
    """
    Analyzes the severity of a single row of metric data based on the configuration.
    Accepts all_findings to enable cross-module checks.
    """
    for config_name, config in METRIC_ANALYSIS_CONFIG.items():
        if any(keyword in metric_name.lower() for keyword in config['metric_keywords']):
            if all(cond['key'] in data_row for cond in config['data_conditions'] if cond.get('exists')):
                for rule in config['rules']:
                    try:
                        # FIXED: Pass 'all_structured_findings' into the eval context
                        if eval(rule['expression'], {"data": data_row, "settings": settings, "all_structured_findings": all_findings}):
                            evaluated_reasoning = eval(f"f\"{rule['reasoning']}\"", {"data": data_row, "settings": settings})
                            return {
                                'level': rule['level'],
                                'score': rule['score'],
                                'reasoning': evaluated_reasoning,
                                'recommendations': rule['recommendations']
                            }
                    except Exception as e:
                        print(f"Warning: Error evaluating rule for metric '{metric_name}': {e}")
    return {'level': 'info', 'score': 0, 'reasoning': '', 'recommendations': []}


def generate_dynamic_prompt(all_structured_findings, settings):
    """Generates a dynamic prompt, managing token size by summarizing healthy modules."""
    findings_for_analysis = convert_to_json_serializable(all_structured_findings)

    critical_issues, high_priority_issues, medium_priority_issues = [], [], []
    module_issue_map = {}

    for module_name, module_findings in findings_for_analysis.items():
        module_issue_map[module_name] = {'critical': 0, 'high': 0, 'medium': 0}
        if module_findings.get("status") == "success" and isinstance(module_findings.get("data"), dict):
            for data_key, data_value in module_findings["data"].items():
                data_list = []
                if 'cloud_metrics' in data_key and isinstance(data_value, dict):
                    for metric_name, metric_data in data_value.items():
                        if isinstance(metric_data, dict) and 'value' in metric_data and isinstance(metric_data['value'], (int, float)):
                            analysis = analyze_metric_severity(f"{module_name}_{data_key}_{metric_name}", metric_data, settings, findings_for_analysis)
                            if analysis['level'] in ['critical', 'high', 'medium']:
                                issue_details = {'metric': f"AWS.{metric_name}", 'analysis': analysis, 'data': metric_data}
                                module_issue_map[module_name][analysis['level']] += 1
                                if analysis['level'] == 'critical': critical_issues.append(issue_details)
                                elif analysis['level'] == 'high': high_priority_issues.append(issue_details)
                                elif analysis['level'] == 'medium': medium_priority_issues.append(issue_details)
                    continue

                if isinstance(data_value, dict) and isinstance(data_value.get('data'), list): data_list = data_value['data']
                elif isinstance(data_value, list): data_list = data_value
                
                for row in data_list:
                    if isinstance(row, dict):
                        # MODIFIED: Pass the full findings dictionary to the analyzer
                        analysis = analyze_metric_severity(f"{module_name}_{data_key}", row, settings, findings_for_analysis)
                        if analysis['level'] in ['critical', 'high', 'medium']:
                            issue_details = {'metric': f"{module_name}_{data_key}", 'analysis': analysis, 'data': row}
                            module_issue_map[module_name][analysis['level']] += 1
                            if analysis['level'] == 'critical': critical_issues.append(issue_details)
                            elif analysis['level'] == 'high': high_priority_issues.append(issue_details)
                            elif analysis['level'] == 'medium': medium_priority_issues.append(issue_details)

    # (Smart summarization and template rendering logic remains the same)
    TOKEN_CHARACTER_RATIO = 4
    max_prompt_tokens = settings.get('ai_max_prompt_tokens', 8000)
    token_budget = max_prompt_tokens * TOKEN_CHARACTER_RATIO
    findings_for_prompt = {}
    estimated_size = 0
    for module_name, issues in module_issue_map.items():
        if issues['critical'] > 0 or issues['high'] > 0:
            findings_for_prompt[module_name] = findings_for_analysis[module_name]
            estimated_size += len(json.dumps(findings_for_analysis[module_name]))
    for module_name, module_data in findings_for_analysis.items():
        if module_name not in findings_for_prompt:
            module_size = len(json.dumps(module_data))
            if estimated_size + module_size < token_budget:
                findings_for_prompt[module_name] = module_data
                estimated_size += module_size
            else:
                findings_for_prompt[module_name] = {
                    "status": "success",
                    "note": "Data for this module was summarized due to prompt size limits. No critical or high-priority issues were detected."
                }
    template_dir = Path(__file__).parent.parent / 'templates'
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(template_dir)), trim_blocks=True, lstrip_blocks=True)
    template_name = settings.get('prompt_template', 'prompt_template.j2')
    template = env.get_template(template_name)
    overview_data = findings_for_analysis.get('postgres_overview', {}).get('data', {})
    version_info_data = overview_data.get('version_info', {}).get('data', [{}])[0]
    database_size_data = overview_data.get('database_size', {}).get('data', [{}])[0]
    postgres_version = version_info_data.get('version', 'N/A')
    database_name = database_size_data.get('database', 'N/A')
    analysis_timestamp = datetime.utcnow().isoformat() + "Z"
    prompt = template.render(
        findings_json=json.dumps(findings_for_prompt, indent=2),
        settings=settings,
        postgres_version=postgres_version,
        database_name=database_name,
        analysis_timestamp=analysis_timestamp,
        critical_issues=critical_issues,
        high_priority_issues=high_priority_issues
    )

    return {
        'prompt': prompt,
        'critical_issues': critical_issues,
        'high_priority_issues': high_priority_issues,
        'medium_priority_issues': medium_priority_issues,
        'total_issues': len(critical_issues) + len(high_priority_issues) + len(medium_priority_issues)
    }
