import json
import psycopg2
import jinja2
from flask import current_app
from .utils import load_trends_config

def estimate_tokens(text):
    """
    Estimates the number of tokens in a text string.

    Uses a configurable heuristic stored in the database (metric table).
    Default: ~4 characters per token (common for English).
    This is a rough estimate - actual tokenization varies by model.

    Args:
        text (str): The text to estimate tokens for

    Returns:
        int: Estimated token count
    """
    if not text:
        return 0

    # Fetch configurable chars-per-token ratio from database
    # This allows admins to tune estimation accuracy per their workload
    from .metrics import get_metric_int, MetricKeys
    chars_per_token = get_metric_int(
        MetricKeys.TOKEN_ESTIMATION_CHARS_PER_TOKEN,
        default=4
    )

    return len(text) // chars_per_token

def analyze_metric_severity(metric_name, data_row, all_findings, analysis_rules):
    """
    Analyzes the severity of a single row of metric data based on rules
    fetched from the database.
    """
    for config_name, config in analysis_rules.items():
        if any(keyword in metric_name.lower() for keyword in config.get('metric_keywords', [])):
            if all(cond.get('key') in data_row for cond in config.get('data_conditions', []) if cond.get('exists')):
                for rule in config.get('rules', []):
                    try:
                        settings = load_trends_config() or {}
                        if eval(rule['expression'], {"data": data_row, "all_structured_findings": all_findings, "settings": settings}):
                            evaluated_reasoning = eval(f"f\"{rule['reasoning']}\"", {"data": data_row, "settings": settings})
                            return {
                                'level': rule.get('level', 'info'),
                                'score': rule.get('score', 0),
                                'reasoning': evaluated_reasoning,
                                'recommendations': rule.get('recommendations', [])
                            }
                    except Exception as e:
                        current_app.logger.warning(f"Warning: Error evaluating rule for metric '{metric_name}': {e}")
    return None

def generate_web_prompt(findings_json, rule_set_id, template_id):
    """
    Generates a dynamic AI prompt by analyzing findings and rendering a Jinja2
    template fetched from the database.
    """
    settings = load_trends_config()
    db_config = settings.get('database')
    if not db_config:
        return "Error: Database configuration not found."

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute("SELECT rules_json, technology FROM analysis_rules WHERE id = %s;", (rule_set_id,))
        rules_data = cursor.fetchone()
        if not rules_data:
            return f"Error: Analysis Rule Set with ID {rule_set_id} not found."
        analysis_rules, technology = rules_data
        
        cursor.execute("SELECT template_content FROM prompt_templates WHERE id = %s;", (template_id,))
        template_data = cursor.fetchone()
        if not template_data:
            return f"Error: Prompt Template with ID {template_id} not found."
        template_content = template_data[0]

        critical_issues, high_priority_issues = [], []
        for module_name, module_findings in findings_json.items():
            if not isinstance(module_findings, dict) or module_findings.get("status") != "success":
                continue
            data_to_scan = module_findings.get("data", {})
            if isinstance(data_to_scan, dict):
                for data_key, data_value in data_to_scan.items():
                    data_list = []
                    if isinstance(data_value, dict) and isinstance(data_value.get('data'), list):
                        data_list = data_value['data']
                    elif isinstance(data_value, list):
                        data_list = data_value
                    for row in data_list:
                        if isinstance(row, dict):
                            analysis = analyze_metric_severity(f"{module_name}_{data_key}", row, findings_json, analysis_rules)
                            if analysis and analysis['level'] in ['critical', 'high']:
                                issue_details = {'metric': f"{module_name}_{data_key}", 'analysis': analysis, 'data': row}
                                if analysis['level'] == 'critical':
                                    critical_issues.append(issue_details)
                                else:
                                    high_priority_issues.append(issue_details)
        
        template = jinja2.Template(template_content)
        
        def get_generic_metadata(findings):
            db_version, db_name = "N/A", "N/A"
            search_order = ['overview', 'version_info', 'cluster_info']
            for module_key in search_order:
                for key, module in findings.items():
                    if module_key in key.lower() and isinstance(module, dict) and "data" in module:
                        data = module["data"]
                        if isinstance(data, dict):
                            for sub_key, sub_val in data.items():
                                if "version" in sub_key and isinstance(sub_val, str): db_version = sub_val
                                if "database" in sub_key or "db_name" in sub_key and isinstance(sub_val, str): db_name = sub_val
                if db_version != "N/A" and db_name != "N/A": break
            return db_version, db_name

        db_version, db_name = get_generic_metadata(findings_json)

        template_context = {
            "findings_json": json.dumps(findings_json, indent=2),
            "settings": settings,
            "technology": technology,
            "db_version": db_version,
            "database_name": db_name,
            "critical_issues": critical_issues,
            "high_priority_issues": high_priority_issues,
            "postgres_version": db_version 
        }

        prompt = template.render(template_context)
        return prompt

    except Exception as e:
        current_app.logger.error(f"An unexpected error occurred in prompt generator: {e}")
        return "Error: An unexpected error occurred."
    finally:
        if conn:
            conn.close()

def generate_bulk_analysis_prompt(runs_data, analysis_style='default'):
    """
    Generates a prompt for analyzing multiple health check runs.

    Args:
        runs_data: List of run dictionaries with keys: id, company_name, target_host,
                   target_port, target_db_name, db_technology, run_timestamp,
                   critical_count, high_count, medium_count, findings
        analysis_style: Style of analysis - 'default', 'technical', 'executive', 'troubleshooting'

    Returns:
        Rendered Jinja2 prompt string or error message
    """
    settings = load_trends_config()
    db_config = settings.get('database')
    conn = None

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Fetch the Dashboard Bulk Analysis template
        cursor.execute("""
            SELECT id, template_content
            FROM prompt_templates
            WHERE template_name = 'Dashboard Bulk Analysis'
            LIMIT 1;
        """)
        template_data = cursor.fetchone()

        if not template_data:
            return "Error: Dashboard Bulk Analysis template not found in database."

        template_id, template_content = template_data

        # Prepare template context
        companies = set()
        technologies = set()
        timestamps = []

        for run in runs_data:
            companies.add(run.get('company_name', 'Unknown'))
            technologies.add(run.get('db_technology', 'Unknown'))
            timestamps.append(run.get('run_timestamp'))

        # Calculate date range
        timestamps_sorted = sorted([ts for ts in timestamps if ts])
        date_range = "Unknown"
        if timestamps_sorted:
            start_date = timestamps_sorted[0].strftime('%Y-%m-%d %H:%M') if hasattr(timestamps_sorted[0], 'strftime') else str(timestamps_sorted[0])
            end_date = timestamps_sorted[-1].strftime('%Y-%m-%d %H:%M') if hasattr(timestamps_sorted[-1], 'strftime') else str(timestamps_sorted[-1])
            date_range = f"{start_date} to {end_date}"

        # Prepare findings data (triggered rules only - much smaller!)
        # Instead of sending ALL raw findings, we send only the triggered rules
        # This reduces token usage from ~300K to ~5-10K per run
        findings_data_parts = []
        for idx, run in enumerate(runs_data, 1):
            run_id = run.get('id')

            # Fetch triggered rules for this run from database
            triggered_rules = []
            try:
                cursor.execute("""
                    SELECT check_name, metric_name, severity_level, reasoning, recommendations
                    FROM health_check_triggered_rules
                    WHERE run_id = %s
                    ORDER BY
                        CASE severity_level
                            WHEN 'critical' THEN 1
                            WHEN 'high' THEN 2
                            WHEN 'medium' THEN 3
                            ELSE 4
                        END,
                        check_name;
                """, (run_id,))
                triggered_rules = cursor.fetchall()
            except Exception as e:
                current_app.logger.error(f"Error fetching triggered rules for run {run_id}: {e}")

            # Format triggered rules as a concise summary
            if triggered_rules:
                rules_summary = []
                for rule in triggered_rules:
                    check_name, metric_name, severity, reasoning, recommendations = rule
                    rules_summary.append({
                        "check": check_name,
                        "metric": metric_name,
                        "severity": severity,
                        "issue": reasoning,
                        "recommendations": recommendations
                    })
                findings_data_parts.append(
                    f"## Run {idx} - Triggered Rules ({len(rules_summary)} issues)\n"
                    f"```json\n{json.dumps(rules_summary, indent=2)}\n```\n"
                )
            else:
                # Check if the run summary indicates issues exist
                total_issues = run.get('critical_count', 0) + run.get('high_count', 0) + run.get('medium_count', 0)
                if total_issues > 0:
                    findings_data_parts.append(
                        f"## Run {idx} - Issues Summary\n"
                        f"**Note:** Run summary indicates {total_issues} total issues "
                        f"({run.get('critical_count', 0)} critical, {run.get('high_count', 0)} high, "
                        f"{run.get('medium_count', 0)} medium), but detailed triggered rules are not available "
                        f"in the database. This may indicate:\n"
                        f"1. This run was created before triggered rules tracking was implemented\n"
                        f"2. The health check analysis rules may need to be updated\n"
                        f"3. The issues may be in the raw findings data but weren't categorized\n\n"
                        f"**Recommendation:** Re-run this health check to populate triggered rules, "
                        f"or review the complete findings data for this run ID: {run_id}\n"
                    )
                else:
                    findings_data_parts.append(f"## Run {idx} - No Issues Found\n")

        if not findings_data_parts:
            findings_data_parts.append("No triggered rules found across all selected runs.")

        # Define style-specific guidance
        style_guidance = {
            'default': {
                'focus': 'balanced analysis with actionable recommendations',
                'tone': 'professional and concise',
                'sections': ['Summary', 'Key Issues', 'Recommendations']
            },
            'technical': {
                'focus': 'deep technical analysis with detailed metrics and root cause analysis',
                'tone': 'technical and detailed',
                'sections': ['Technical Overview', 'Performance Metrics', 'Configuration Analysis', 'Root Cause Analysis', 'Technical Recommendations']
            },
            'executive': {
                'focus': 'business impact and strategic recommendations',
                'tone': 'executive-friendly with minimal technical jargon',
                'sections': ['Executive Summary', 'Business Impact', 'Risk Assessment', 'Strategic Recommendations']
            },
            'troubleshooting': {
                'focus': 'problem identification and step-by-step resolution guidance',
                'tone': 'prescriptive and actionable',
                'sections': ['Problem Identification', 'Impact Assessment', 'Resolution Steps', 'Verification Procedures']
            }
        }

        # Get guidance for selected style (default to 'default' if style not found)
        selected_guidance = style_guidance.get(analysis_style, style_guidance['default'])

        template_context = {
            "run_count": len(runs_data),
            "companies": ", ".join(sorted(companies)),
            "technologies": list(sorted(technologies)),
            "date_range": date_range,
            "runs": runs_data,
            "findings_data": "\n".join(findings_data_parts),
            "analysis_style": analysis_style,
            "style_focus": selected_guidance['focus'],
            "style_tone": selected_guidance['tone'],
            "style_sections": selected_guidance['sections']
        }

        template = jinja2.Template(template_content)
        prompt = template.render(template_context)
        return prompt

    except Exception as e:
        current_app.logger.error(f"Error generating bulk analysis prompt: {e}")
        return f"Error: Could not generate bulk analysis prompt: {e}"
    finally:
        if conn:
            conn.close()

def generate_slides_prompt(findings_json, rule_set_id, template_id, assets=None):
    """
    Generates a prompt for creating presentation slides by rendering a Jinja2 template.
    This version accepts an 'assets' dictionary containing local file paths.
    """
    if assets is None:
        assets = {}

    settings = load_trends_config()
    db_config = settings.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute("SELECT rules_json, technology FROM analysis_rules WHERE id = %s;", (rule_set_id,))
        rules_data = cursor.fetchone()
        if not rules_data:
            return "Error: Analysis Rule Set not found."
        analysis_rules, technology = rules_data

        cursor.execute("SELECT template_content FROM prompt_templates WHERE id = %s;", (template_id,))
        template_data = cursor.fetchone()
        if not template_data:
            return f"Error: Slide Template with ID {template_id} not found."
        template_content = template_data[0]

        # CORRECTED: The block that generated URLs has been completely removed.
        # This function now relies entirely on the 'assets' dictionary passed in from main.py.

        # Use the same rich data processing as generate_web_prompt for high-quality AI output
        critical_issues, high_priority_issues = [], []
        for module_name, module_findings in findings_json.items():
            if not isinstance(module_findings, dict) or module_findings.get("status") != "success": continue
            data_to_scan = module_findings.get("data", {})
            if isinstance(data_to_scan, dict):
                for data_key, data_value in data_to_scan.items():
                    data_list = data_value.get('data', []) if isinstance(data_value, dict) else data_value if isinstance(data_value, list) else []
                    for row in data_list:
                        if isinstance(row, dict):
                            analysis = analyze_metric_severity(f"{module_name}_{data_key}", row, findings_json, analysis_rules)
                            if analysis and analysis['level'] in ['critical', 'high']:
                                # Create a rich issue dictionary, including the raw data
                                issue_details = {
                                    'metric': f"{module_name}_{data_key}",
                                    'analysis': analysis,
                                    'data': row
                                }
                                if analysis['level'] == 'critical':
                                    critical_issues.append(issue_details)
                                else:
                                    high_priority_issues.append(issue_details)

        template = jinja2.Template(template_content)
        template_context = {
            "critical_issues": critical_issues,
            "high_priority_issues": high_priority_issues,
            "technology": technology,
            "findings_json": json.dumps(findings_json, indent=2),
            "assets": assets
        }
        prompt = template.render(template_context)
        return prompt

    except Exception as e:
        current_app.logger.error(f"Error generating slides prompt: {e}")
        return "Error: Could not generate the slides prompt."
    finally:
        if conn: conn.close()
