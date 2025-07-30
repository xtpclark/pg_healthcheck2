#!/usr/bin/env python3
"""
Dynamic Prompt Generator for Health Check AI Analysis

This module analyzes structured findings and generates context-aware prompts
based on the severity and type of issues detected in the database.
It uses a weighted token budgeting strategy to ensure the prompt fits within
the AI model's context window.
"""

import json
import copy
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

def analyze_metric_severity(metric_name, data_row, settings, all_findings, analysis_rules, rule_stats, verbose=False):
    """
    Analyzes the severity of a single row of metric data based on all applicable rules,
    returning the finding with the highest severity.
    """
    highest_severity_finding = {'level': 'info', 'score': 0, 'reasoning': '', 'recommendations': []}

    for config_name, config in analysis_rules.items():
        keyword_match = any(keyword in metric_name.lower() for keyword in config.get('metric_keywords', []))
        
        if verbose and keyword_match:
            print(f"\n[DEBUG] METRIC: '{metric_name}' | RULE_CONFIG: '{config_name}'")
            print(f"  - Keywords: {config.get('metric_keywords', [])} -> MATCH")

        if keyword_match:
            conditions_met = all(cond.get('key') in data_row for cond in config.get('data_conditions', []) if cond.get('exists'))
            if verbose:
                 print(f"  - Conditions: {config.get('data_conditions', [])} -> {'MET' if conditions_met else 'NOT MET'}")

            if conditions_met:
                for rule in config.get('rules', []):
                    if config_name not in rule_stats:
                        rule_stats[config_name] = {'checked': 0, 'triggered': 0, 'errors': 0}
                    
                    try:
                        rule_stats[config_name]['checked'] += 1
                        expression_result = eval(rule['expression'], {"data": data_row, "settings": settings, "all_structured_findings": all_findings})
                        
                        if verbose:
                            print(f"    - Evaluating Rule: level='{rule.get('level')}'")
                            print(f"      - Expression: {rule['expression']}")
                            print(f"      - DATA_ROW: {json.dumps(data_row, indent=2, default=str)}")
                            print(f"      - RESULT: {'TRIGGERED' if expression_result else 'NOT TRIGGERED'}")

                        if expression_result:
                            rule_stats[config_name]['triggered'] += 1
                            evaluated_reasoning = eval(f"f\"{rule['reasoning']}\"", {"data": data_row, "settings": settings})
                            current_finding = {
                                'level': rule.get('level', 'info'),
                                'score': rule.get('score', 0),
                                'reasoning': evaluated_reasoning,
                                'recommendations': rule.get('recommendations', [])
                            }
                            if current_finding['score'] > highest_severity_finding['score']:
                                highest_severity_finding = current_finding
                                
                    except Exception as e:
                        rule_stats[config_name]['errors'] += 1
                        print(f"Warning: Error evaluating rule '{config_name}' for metric '{metric_name}': {e}")

    return highest_severity_finding


def _process_findings_recursively(current_findings, settings, analysis_rules, all_findings, rule_stats, issue_lists, module_issue_map, parent_key='', verbose=False):
    """
    A recursive helper to process potentially nested finding structures.
    """
    critical_issues, high_priority_issues, medium_priority_issues = issue_lists

    for key, value in current_findings.items():
        if not isinstance(value, dict):
            continue

        metric_name = f"{parent_key}_{key}" if parent_key else key

        if 'status' in value and value.get('status') == 'success':
            data = value.get('data', {})
            data_list = []

            if isinstance(data, list):
                data_list = data
            elif isinstance(data, dict):
                is_list_like = any(isinstance(v, list) for v in data.values())
                if not is_list_like:
                    data_list = [data]
                else:
                    _process_findings_recursively(data, settings, analysis_rules, all_findings, rule_stats, issue_lists, module_issue_map, parent_key=metric_name, verbose=verbose)
                    continue

            for row in data_list:
                if isinstance(row, dict):
                    analysis = analyze_metric_severity(metric_name, row, settings, all_findings, analysis_rules, rule_stats, verbose=verbose)
                    if analysis['level'] in ['critical', 'high', 'medium']:
                        module_name = parent_key or key
                        if module_name not in module_issue_map:
                             module_issue_map[module_name] = {'critical': 0, 'high': 0, 'medium': 0}
                        
                        issue_details = {'metric': metric_name, 'analysis': analysis, 'data': row}
                        module_issue_map[module_name][analysis['level']] += 1
                        
                        if analysis['level'] == 'critical': critical_issues.append(issue_details)
                        elif analysis['level'] == 'high': high_priority_issues.append(issue_details)
                        elif analysis['level'] == 'medium': medium_priority_issues.append(issue_details)

        elif 'status' not in value:
            _process_findings_recursively(value, settings, analysis_rules, all_findings, rule_stats, issue_lists, module_issue_map, parent_key=metric_name, verbose=verbose)


def generate_dynamic_prompt(all_structured_findings, settings, analysis_rules, db_version, db_name, active_plugin, verbose=False):
    """
    Generates a dynamic prompt using a weighted budgeting strategy.
    """
    findings_for_analysis = convert_to_json_serializable(all_structured_findings)
    rule_stats = {}
    critical_issues, high_priority_issues, medium_priority_issues = [], [], []
    issue_lists = (critical_issues, high_priority_issues, medium_priority_issues)
    module_issue_map = {}

    _process_findings_recursively(findings_for_analysis, settings, analysis_rules, findings_for_analysis, rule_stats, issue_lists, module_issue_map, verbose=verbose)

    # --- Weighted Token Budgeting Logic ---
    TOKEN_CHARACTER_RATIO = 4
    max_prompt_tokens = settings.get('ai_max_prompt_tokens', 8000)
    token_budget = max_prompt_tokens * TOKEN_CHARACTER_RATIO
    findings_for_prompt = {}
    estimated_size = 0

    # Stage 1: Add all modules with critical or high-priority issues in full
    for module_name, issues in module_issue_map.items():
        if issues['critical'] > 0 or issues['high'] > 0:
            if module_name in findings_for_analysis:
                findings_for_prompt[module_name] = findings_for_analysis[module_name]
                estimated_size += len(json.dumps(findings_for_analysis[module_name]))

    # Stage 2: Perform weighted, proportional sampling for remaining modules
    remaining_findings = copy.deepcopy(findings_for_analysis)
    module_weights = active_plugin.get_module_weights()

    for module_name in findings_for_prompt:
        if module_name in remaining_findings:
            del remaining_findings[module_name]

    total_weight = sum(module_weights.get(name, 1) for name in remaining_findings)
    remaining_budget = token_budget - estimated_size

    if total_weight > 0:
        for module_name, module_data in remaining_findings.items():
            weight = module_weights.get(module_name, 1)
            proportional_budget = (weight / total_weight) * remaining_budget
            
            # Iteratively trim lists until the module fits its budget
            while len(json.dumps(module_data)) > proportional_budget:
                trimmed = False
                if 'data' in module_data and isinstance(module_data.get('data'), list) and len(module_data['data']) > 1:
                    module_data['data'].pop()
                    trimmed = True
                elif 'data' in module_data and isinstance(module_data.get('data'), dict):
                    for data_key, data_value in module_data['data'].items():
                        if isinstance(data_value, dict) and isinstance(data_value.get('data'), list) and len(data_value['data']) > 1:
                            data_value['data'].pop()
                            trimmed = True
                if not trimmed:
                    break 

            findings_for_prompt[module_name] = module_data

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
        high_priority_issues=high_priority_issues,
        medium_priority_issues=medium_priority_issues
    )

    return {
        'prompt': prompt,
        'summarized_findings': findings_for_prompt,
        'critical_issues': critical_issues,
        'high_priority_issues': high_priority_issues,
        'medium_priority_issues': medium_priority_issues,
        'total_issues': len(critical_issues) + len(high_priority_issues) + len(medium_priority_issues),
        'rule_application_stats': rule_stats
    }
