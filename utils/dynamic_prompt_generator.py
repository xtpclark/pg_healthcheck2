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
    Generates a dynamic prompt using a weighted budgeting strategy and reports on trimmed content.
    """
    findings_for_analysis = convert_to_json_serializable(all_structured_findings)
    rule_stats = {}
    critical_issues, high_priority_issues, medium_priority_issues = [], [], []
    issue_lists = (critical_issues, high_priority_issues, medium_priority_issues)
    module_issue_map = {}

    _process_findings_recursively(findings_for_analysis, settings, analysis_rules, findings_for_analysis, rule_stats, issue_lists, module_issue_map, verbose=verbose)


# --- Weighted Token Budgeting Logic ---
    TOKEN_CHARACTER_RATIO = 4
    # Reserve 4000 characters (~1000 tokens) for prompt instructions, headers, and summaries.
    RESERVED_BUFFER_FOR_PROMPT_OVERHEAD = 4000 
    
    max_prompt_tokens = settings.get('ai_max_prompt_tokens', 8000)
    # The total budget for the entire prompt string
    total_character_budget = max_prompt_tokens * TOKEN_CHARACTER_RATIO
    # The budget for just the findings_json part is the total minus our buffer
    token_budget = total_character_budget - RESERVED_BUFFER_FOR_PROMPT_OVERHEAD

    
    # Create a single list of all modules with a calculated priority score
    module_weights = active_plugin.get_module_weights()
    all_modules_with_priority = []
    for module_name, module_data in findings_for_analysis.items():
        priority_score = module_weights.get(module_name, 1)
        if module_name in module_issue_map:
            if module_issue_map[module_name]['critical'] > 0:
                priority_score += 1000  # High boost for critical issues
            if module_issue_map[module_name]['high'] > 0:
                priority_score += 100   # Medium boost for high-priority issues
        all_modules_with_priority.append({'name': module_name, 'data': module_data, 'priority': priority_score})

    # Sort all modules by the new priority score, descending
    sorted_modules = sorted(all_modules_with_priority, key=lambda x: x['priority'], reverse=True)

    # --- Single Loop for Prompt Assembly ---
    current_size = 0
    findings_for_prompt = {}
    trimmed_modules_log = {}

    for module in sorted_modules:
        module_name = module['name']
        # Use a deep copy to ensure original findings aren't modified
        module_data = copy.deepcopy(module['data'])

        # First, check if the UNTRIMMED module fits
        original_module_size = len(json.dumps(module_data))
        if (current_size + original_module_size) <= token_budget:
            findings_for_prompt[module_name] = module_data
            current_size += original_module_size
            continue

        # If it doesn't fit, try to trim it
        trim_details = []
        for sub_report_name, sub_report_data in module_data.items():
            if isinstance(sub_report_data, dict) and 'data' in sub_report_data and isinstance(sub_report_data['data'], list) and len(sub_report_data['data']) > 1:
                original_len = len(sub_report_data['data'])
                sub_report_data['data'] = sub_report_data['data'][:1]
                trim_details.append(f"  - List '{sub_report_name}' trimmed from {original_len} to 1 items.")
        
        trimmed_module_size = len(json.dumps(module_data))

        # Check if the TRIMMED version now fits
        if (current_size + trimmed_module_size) <= token_budget:
            findings_for_prompt[module_name] = module_data
            current_size += trimmed_module_size
            if trim_details:
                trimmed_modules_log[module_name] = trim_details
    
    # Log the results of trimming actions
    if trimmed_modules_log:
        print("\n--- Prompt Content Trimming Summary ---")
        for module_name, details in trimmed_modules_log.items():
            print(f"Module '{module_name}':")
            for detail in details:
                print(detail)
    
    if len(findings_for_prompt) < len(all_structured_findings):
        print(f"[INFO] Token budget enforced. Some modules may have been skipped or trimmed to meet the {max_prompt_tokens} token limit.")

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


