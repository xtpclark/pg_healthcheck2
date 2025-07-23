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

# This file is now completely generic and has no database-specific logic.

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
                        # Use eval to dynamically check rules against the data
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


def generate_dynamic_prompt(all_structured_findings, settings, analysis_rules, db_version, db_name):
    """
    Generates a dynamic prompt by passing in technology-specific details.
    """
    findings_for_analysis = convert_to_json_serializable(all_structured_findings)
    critical_issues, high_priority_issues, medium_priority_issues = [], [], []
    module_issue_map = {}

    # Severity analysis loop (remains the same)
    for module_name, module_findings in findings_for_analysis.items():
        # ... (This logic is generic and does not need to change)

    # Smart summarization logic (remains the same)
    # ...

    # --- Template Rendering (Now fully generic) ---
    template_dir = Path(__file__).parent.parent / 'templates'
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(template_dir)), trim_blocks=True, lstrip_blocks=True)
    template_name = settings.get('prompt_template', 'prompt_template.j2')
    template = env.get_template(template_name)
    
    analysis_timestamp = datetime.utcnow().isoformat() + "Z"

    # The prompt now uses the generic db_version and db_name passed into the function
    prompt = template.render(
        findings_json=json.dumps(findings_for_prompt, indent=2),
        settings=settings,
        db_version=db_version,       # <-- Use passed-in argument
        database_name=db_name,       # <-- Use passed-in argument
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
