import json
import psycopg2
import jinja2
from flask import current_app, url_for
from .utils import load_trends_config

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

def generate_slides_prompt(findings_json, rule_set_id, template_id):
    """
    Generates a prompt for creating presentation slides by rendering a Jinja2 template.
    """
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

        assets = {}
        with current_app.app_context():
            cursor.execute("SELECT asset_name FROM template_assets;")
            for row in cursor.fetchall():
                asset_name = row[0]
                assets[asset_name] = url_for('admin.get_template_asset', asset_name=asset_name, _external=True)

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
                                recommendations = analysis.get('recommendations', [])
                                issue = {
                                    'metric': f"{module_name}_{data_key}",
                                    'reasoning': analysis['reasoning'],
                                    'recommendations': recommendations,
                                    'primary_recommendation': recommendations[0] if recommendations else "Review metric details and consult documentation."
                                }
                                if analysis['level'] == 'critical':
                                    critical_issues.append(issue)
                                else:
                                    high_priority_issues.append(issue)

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
