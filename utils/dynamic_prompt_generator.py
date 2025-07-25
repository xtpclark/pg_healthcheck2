#!/usr/bin/env python3
"""
Dynamic Prompt Generator for Health Check AI Analysis

This module analyzes structured findings and generates context-aware prompts
based on the severity and type of issues detected in the database.
It is completely technology-agnostic and relies on the calling script
to provide specific metadata and analysis rules.
"""

import json
from decimal import Decimal
from datetime import datetime, timedelta
import jinja2
from pathlib import Path


def convert_to_json_serializable(obj):
    """Convert non-JSON-serializable objects to JSON-compatible types."""
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, datetime): return obj.isoformat()
    if isinstance(obj, timedelta): return obj.total_seconds()
    if isinstance(obj, dict): return {k: convert_to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list): return [convert_to_json_serializable(item) for item in obj]
    return obj

def analyze_metric_severity(metric_name, data_row, settings, all_findings, analysis_rules):
    """
    Analyzes the severity of a single row of metric data based on the provided rules.
    """
    for config_name, config in analysis_rules.items():
        if any(keyword in metric_name.lower() for keyword in config.get('metric_keywords', [])):
            if all(cond.get('key') in data_row for cond in config.get('data_conditions', []) if cond.get('exists')):
                for rule in config.get('rules', []):
                    try:
                        if eval(rule['expression'], {"data": data_row, "settings": settings, "all_structured_findings": all_findings}):
                            evaluated_reasoning = eval(f"f\"{rule['reasoning']}\"", {"data": data_row, "settings": settings})
                            return {
                                'level': rule.get('level', 'info'),
                                'score': rule.get('score', 0),
                                'reasoning': evaluated_reasoning,
                                'recommendations': rule.get('recommendations', [])
                            }
                    except Exception as e:
                        print(f"Warning: Error evaluating rule for metric '{metric_name}': {e}")
    return {'level': 'info', 'score': 0, 'reasoning': '', 'recommendations': []}


def generate_dynamic_prompt(all_structured_findings, settings, analysis_rules, db_version, db_name, active_plugin):
    """
    Generates a dynamic prompt by passing in technology-specific details.
    """
    findings_for_analysis = convert_to_json_serializable(all_structured_findings)
    critical_issues, high_priority_issues, medium_priority_issues = [], [], []
    module_issue_map = {}

    # Severity analysis loop
    for module_name, module_findings in findings_for_analysis.items():
        # --- START OF CORRECTED INDENTATION ---
        module_issue_map[module_name] = {'critical': 0, 'high': 0, 'medium': 0}
        if module_findings.get("status") == "success" and isinstance(module_findings.get("data"), dict):
            for data_key, data_value in module_findings["data"].items():
                data_list = []
                if 'cloud_metrics' in data_key and isinstance(data_value, dict):
                    for metric_name, metric_data in data_value.items():
                        if isinstance(metric_data, dict) and 'value' in metric_data and isinstance(metric_data['value'], (int, float)):
                            analysis = analyze_metric_severity(f"{module_name}_{data_key}_{metric_name}", metric_data, settings, findings_for_analysis, analysis_rules)
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
                        analysis = analyze_metric_severity(f"{module_name}_{data_key}", row, settings, findings_for_analysis, analysis_rules)
                        if analysis['level'] in ['critical', 'high', 'medium']:
                            issue_details = {'metric': f"{module_name}_{data_key}", 'analysis': analysis, 'data': row}
                            module_issue_map[module_name][analysis['level']] += 1
                            if analysis['level'] == 'critical': critical_issues.append(issue_details)
                            elif analysis['level'] == 'high': high_priority_issues.append(issue_details)
                            elif analysis['level'] == 'medium': medium_priority_issues.append(issue_details)
    # --- END OF CORRECTED INDENTATION ---

    # Smart summarization logic
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
    
    # --- Template Rendering ---
    template_dir = active_plugin.get_template_path() / "prompts"
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(template_dir)), trim_blocks=True, lstrip_blocks=True)
    template_name = settings.get('prompt_template', 'default_prompt.j2')
    template = env.get_template(template_name)
    
    analysis_timestamp = datetime.utcnow().isoformat() + "Z"

    prompt = template.render(
        findings_json=json.dumps(findings_for_prompt, indent=2),
        settings=settings,
        db_version=db_version,
        database_name=db_name,
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
