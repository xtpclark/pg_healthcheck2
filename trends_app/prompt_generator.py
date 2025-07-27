import json
import psycopg2
import jinja2
from flask import current_app
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
                        if eval(rule['expression'], {"data": data_row, "all_structured_findings": all_findings}):
                            evaluated_reasoning = eval(f"f\"{rule['reasoning']}\"", {"data": data_row})
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

    Args:
        findings_json (dict): The structured JSON findings from a health check run.
        rule_set_id (int): The ID of the analysis_rules set to use.
        template_id (int): The ID of the prompt_templates entry to use.

    Returns:
        str: A formatted prompt string for the AI, or an error message.
    """
    config = load_trends_config()
    db_config = config.get('database')
    if not db_config:
        return "Error: Database configuration not found."

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Fetch analysis rules and technology name
        cursor.execute("SELECT rules_json, technology FROM analysis_rules WHERE id = %s;", (rule_set_id,))
        rules_data = cursor.fetchone()
        if not rules_data:
            return f"Error: Analysis Rule Set with ID {rule_set_id} not found."
        analysis_rules, technology = rules_data
        
        # Fetch the prompt template
        cursor.execute("SELECT template_content FROM prompt_templates WHERE id = %s;", (template_id,))
        template_data = cursor.fetchone()
        if not template_data:
            return f"Error: Prompt Template with ID {template_id} not found."
        template_content = template_data[0]

        # --- Perform Severity Analysis ---
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
        
        # --- Render the Jinja2 Template ---
        template = jinja2.Template(template_content)
        
        # --- Generic Context Variable Preparation ---
        def get_generic_metadata(findings):
            """
            Dynamically finds common metadata to make the prompt generator
            agnostic to the underlying database technology. It searches for
            keys containing common terms.
            """
            db_version = "N/A"
            db_name = "N/A"
            # Search for version and name in a prioritized order of modules if possible
            search_order = ['overview', 'version_info', 'cluster_info']
            
            # First pass with prioritized modules
            for module_key in search_order:
                for key, module in findings.items():
                    if module_key in key.lower():
                         if isinstance(module, dict) and "data" in module:
                            data = module["data"]
                            if isinstance(data, dict):
                                for sub_key, sub_val in data.items():
                                    if "version" in sub_key and isinstance(sub_val, str):
                                        db_version = sub_val
                                    if "database" in sub_key or "db_name" in sub_key and isinstance(sub_val, str):
                                        db_name = sub_val
                if db_version != "N/A" and db_name != "N/A":
                    break # Stop if we found both

            return db_version, db_name

        db_version, db_name = get_generic_metadata(findings_json)

        # Create a context dictionary for rendering the template
        template_context = {
            "findings_json": json.dumps(findings_json, indent=2),
            "technology": technology,
            "db_version": db_version,
            "database_name": db_name,
            "critical_issues": critical_issues,
            "high_priority_issues": high_priority_issues,
            # For backward compatibility with older postgres-specific templates
            "postgres_version": db_version 
        }

        prompt = template.render(template_context)
        return prompt

    except psycopg2.Error as db_err:
        current_app.logger.error(f"Database error in prompt generator: {db_err}")
        return "Error: A database error occurred."
    except jinja2.TemplateError as template_err:
        current_app.logger.error(f"Jinja2 template error: {template_err}")
        return "Error: Could not render the AI prompt template."
    except Exception as e:
        current_app.logger.error(f"An unexpected error occurred in prompt generator: {e}")
        return "Error: An unexpected error occurred."
    finally:
        if conn:
            conn.close()
