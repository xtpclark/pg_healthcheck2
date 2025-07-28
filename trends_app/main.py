from flask import Blueprint, render_template, request, jsonify, current_app, redirect, url_for, abort, send_file
from flask_login import login_required, current_user
import psycopg2
import io
from .database import get_unique_targets, load_user_preferences, fetch_runs_by_ids, save_user_preference
from .utils import load_trends_config, format_path
from .ai_connector import get_ai_recommendation
from .prompt_generator import generate_web_prompt, generate_slides_prompt
import json
from deepdiff import DeepDiff
from datetime import datetime

bp = Blueprint('main', __name__)

@bp.before_request
def before_request_callback():
    """Redirects user to change password if required."""
    if current_user.is_authenticated and current_user.password_change_required:
        if request.endpoint and 'static' not in request.endpoint and 'auth.' not in request.endpoint:
             return redirect(url_for('auth.change_password'))

@bp.app_template_filter('format_path')
def jinja_format_path(path):
    """Makes the format_path function available in templates."""
    return format_path(path)

@bp.route('/')
@login_required
def dashboard():
    """Main dashboard route."""
    config = load_trends_config()
    db_settings = config.get('database')
    
    run_id_1 = request.args.get('run1', type=int)
    run_id_2 = request.args.get('run2', type=int)
    
    runs = []
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]

    if run_id_1 and run_id_2:
        runs = fetch_runs_by_ids(db_settings, [run_id_1, run_id_2], accessible_company_ids)
    
    changes_by_check = {}
    if len(runs) >= 2:
        findings1 = runs[0].get('findings', {})
        findings2 = runs[1].get('findings', {})
        if isinstance(findings1, dict) and isinstance(findings2, dict):
            diff = DeepDiff(findings2, findings1, ignore_order=True, view='tree')
            if diff:
                for change_type, items in diff.items():
                    for item in items:
                        top_level_check = item.path(output_format='list')[0]
                        if top_level_check not in changes_by_check:
                            changes_by_check[top_level_check] = []
                        
                        path_str = format_path(item.path())
                        summary = ""
                        if change_type == 'values_changed':
                            summary = f"🔄 Value at `{path_str}` changed from **'{item.t1}'** to **'{item.t2}'**"
                        elif change_type == 'iterable_item_added':
                            summary = f"➕ Item added to `{path_str}`: `{json.dumps(item.t2)}`"
                        elif change_type == 'iterable_item_removed':
                            summary = f"➖ Item removed from `{path_str}`: `{json.dumps(item.t1)}`"
                        
                        if summary:
                            changes_by_check[top_level_check].append(summary)

    unique_targets = get_unique_targets(db_settings, accessible_company_ids)
    user_preferences = load_user_preferences(db_settings, current_user.username)

    return render_template('dashboard.html', 
                           runs=runs, 
                           changes_by_check=changes_by_check,
                           unique_targets=unique_targets,
                           user_preferences=json.dumps(user_preferences))

# --- API Routes ---

@bp.route('/api/runs')
@login_required
def get_all_runs():
    """API endpoint to fetch runs, with support for filtering and favorites."""
    config = load_trends_config()
    db_settings = config.get('database')
    
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    if not accessible_company_ids:
        return jsonify([])

    target_filter = request.args.get('target')
    start_time_str = request.args.get('start_time')
    end_time_str = request.args.get('end_time')

    conn = None
    all_runs = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        
        params = {
            'company_ids': accessible_company_ids,
            'user_id': current_user.id
        }
        
        query_parts = [
            "SELECT hcr.id, hcr.run_timestamp, hcr.target_host, hcr.target_port, hcr.target_db_name,",
            "CASE WHEN ufr.user_id IS NOT NULL THEN true ELSE false END AS is_favorite",
            "FROM health_check_runs hcr",
            "JOIN companies c ON hcr.company_id = c.id",
            "LEFT JOIN user_favorite_runs ufr ON hcr.id = ufr.run_id AND ufr.user_id = %(user_id)s",
            "WHERE hcr.company_id = ANY(%(company_ids)s)"
        ]

        if target_filter and target_filter != 'all':
            try:
                company_name, host, port, db_name = target_filter.split(':', 3)
                query_parts.append("AND c.company_name = %(company_name)s AND hcr.target_host = %(host)s AND hcr.target_port = %(port)s AND hcr.target_db_name = %(db_name)s")
                params.update({'company_name': company_name, 'host': host, 'port': int(port), 'db_name': db_name})
            except (ValueError, IndexError) as e:
                current_app.logger.warning(f"Invalid target filter format: {target_filter}. Error: {e}")

        if start_time_str:
            query_parts.append("AND hcr.run_timestamp >= %(start_time)s::date")
            params['start_time'] = start_time_str

        if end_time_str:
            query_parts.append("AND hcr.run_timestamp < (%(end_time)s::date + interval '1 day')")
            params['end_time'] = end_time_str

        query_parts.append("ORDER BY hcr.run_timestamp DESC;")
        
        query = " ".join(query_parts)
        cursor.execute(query, params)

        for row in cursor.fetchall():
            all_runs.append({
                "id": row[0],
                "timestamp": row[1].isoformat(),
                "target": f"{row[2]}:{row[3]} ({row[4]})",
                "is_favorite": row[5]
            })
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching all runs with filters: {e}")
    finally:
        if conn: conn.close()
    return jsonify(all_runs)

@bp.route('/api/runs/toggle-favorite', methods=['POST'])
@login_required
def toggle_favorite():
    data = request.get_json()
    run_id = data.get('run_id')

    if not run_id:
        return jsonify({'status': 'error', 'message': 'run_id is required.'}), 400
    
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM user_favorite_runs WHERE user_id = %s AND run_id = %s;", (current_user.id, run_id))
        exists = cursor.fetchone()

        if exists:
            cursor.execute("DELETE FROM user_favorite_runs WHERE user_id = %s AND run_id = %s;", (current_user.id, run_id))
            new_status = False
        else:
            cursor.execute("INSERT INTO user_favorite_runs (user_id, run_id) VALUES (%s, %s);", (current_user.id, run_id))
            new_status = True
        
        conn.commit()
        return jsonify({'status': 'success', 'is_favorite': new_status})

    except psycopg2.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"Database error toggling favorite: {e}")
        return jsonify({'status': 'error', 'message': 'Database error.'}), 500
    finally:
        if conn: conn.close()

@bp.route('/api/save-preference', methods=['POST'])
@login_required
def save_preference():
    data = request.get_json()
    pref_name = data.get('name')
    pref_value = data.get('value')

    if not pref_name or pref_value is None:
        return jsonify({'status': 'error', 'message': 'Missing preference name or value'}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    save_user_preference(db_settings, current_user.username, pref_name, pref_value)
    
    return jsonify({'status': 'success', 'message': f'Preference {pref_name} saved.'})

@bp.route('/api/user-ai-profiles')
@login_required
def get_user_ai_profiles():
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    profiles = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, profile_name FROM user_ai_profiles WHERE user_id = %s ORDER BY profile_name;",
            (current_user.id,)
        )
        for row in cursor.fetchall():
            profiles.append({'id': row[0], 'name': row[1]})
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching AI profiles: {e}")
        return jsonify({"error": "Could not fetch AI profiles."}), 500
    finally:
        if conn: conn.close()
    return jsonify(profiles)

@bp.route('/api/analysis-rules')
@login_required
def get_analysis_rules():
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    rules = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute("SELECT id, rule_set_name FROM analysis_rules ORDER BY rule_set_name;")
        for row in cursor.fetchall():
            rules.append({'id': row[0], 'name': row[1]})
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching analysis rules: {e}")
        return jsonify({"error": "Could not fetch analysis rules."}), 500
    finally:
        if conn: conn.close()
    return jsonify(rules)

@bp.route('/api/prompt-templates')
@login_required
def get_prompt_templates():
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    templates = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, template_name FROM prompt_templates WHERE user_id IS NULL OR user_id = %s ORDER BY template_name;",
            (current_user.id,)
        )
        for row in cursor.fetchall():
            templates.append({'id': row[0], 'name': row[1]})
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching prompt templates: {e}")
        return jsonify({"error": "Could not fetch prompt templates."}), 500
    finally:
        if conn: conn.close()
    return jsonify(templates)

@bp.route('/api/generate-ai-report', methods=['POST'])
@login_required
def generate_ai_report():
    if not current_user.has_privilege('GenerateReports'):
        abort(403)

    data = request.get_json()
    run_id, profile_id, template_id, rule_set_id = data.get('run_id'), data.get('profile_id'), data.get('template_id'), data.get('rule_set_id')
    report_name, report_description = data.get('report_name'), data.get('report_description')

    if not all([run_id, profile_id, template_id, rule_set_id]):
        return jsonify({"error": "Missing required parameters."}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    runs_data = fetch_runs_by_ids(db_settings, [run_id], accessible_company_ids)
    if not runs_data:
        return jsonify({"error": "Run not found or permission denied."}), 404
    findings_json = runs_data[0].get('findings')
    if not isinstance(findings_json, dict):
        return jsonify({"error": "Invalid findings data."}), 500

    prompt = generate_web_prompt(findings_json, rule_set_id, template_id)
    if prompt.startswith("Error:"): return jsonify({"error": prompt}), 500
    ai_response = get_ai_recommendation(prompt, profile_id)
    if ai_response.startswith("Error:"): return jsonify({"error": ai_response}), 500

    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO generated_ai_reports (run_id, rule_set_id, ai_profile_id, generated_by_user_id, report_name, report_description, template_id, report_content) VALUES (%s, %s, %s, %s, %s, %s, %s, pgp_sym_encrypt(%s, get_encryption_key()));",
            (run_id, rule_set_id, profile_id, current_user.id, report_name, report_description, template_id, ai_response)
        )
        conn.commit()
    except psycopg2.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"DB error saving report: {e}")
        return jsonify({"error": "DB error saving report."}), 500
    finally:
        if conn: conn.close()

    return send_file(io.BytesIO(ai_response.encode('utf-8')), mimetype='text/plain', as_attachment=True, download_name=f"ai_report_run_{run_id}.adoc")

# --- MODIFIED: New server-side route for slides with proper error handling ---
@bp.route('/generate-slides')
@login_required
def generate_slides():
    if not current_user.has_privilege('GenerateReports'):
        abort(403)

    run_id = request.args.get('run_id', type=int)
    profile_id = request.args.get('profile_id', type=int)
    rule_set_id = request.args.get('rule_set_id', type=int)
    template_id = request.args.get('template_id', type=int)

    # --- UPDATED ERROR HANDLING ---
    if not all([run_id, profile_id, rule_set_id, template_id]):
        return render_template('error.html', error_message="Missing one or more required parameters (run, profile, rules, or template)."), 400

    config = load_trends_config()
    db_settings = config.get('database')
    
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    runs_data = fetch_runs_by_ids(db_settings, [run_id], accessible_company_ids)
    
    # --- UPDATED ERROR HANDLING ---
    if not runs_data:
        return render_template('error.html', error_message="The requested run was not found or you do not have permission to access it."), 404
    
    findings_json = runs_data[0].get('findings')

    # --- UPDATED ERROR HANDLING ---
    if not isinstance(findings_json, dict):
        return render_template('error.html', error_message="The findings data for this run is invalid or corrupted."), 500

    prompt = generate_slides_prompt(findings_json, rule_set_id, template_id)
    
    # --- UPDATED ERROR HANDLING ---
    if prompt.startswith("Error:"):
        return render_template('error.html', error_message=prompt), 500

    slides_content = get_ai_recommendation(prompt, profile_id)
    
    # --- UPDATED ERROR HANDLING ---
    if slides_content.startswith("Error:"):
        return render_template('error.html', error_message=slides_content), 500

    # Success Case: Render the slides as intended
    return render_template('profile/view_slides.html', 
                           slides_content=slides_content, 
                           run_id=run_id)

@bp.route('/api/all-reports')
@login_required
def get_all_reports():
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    reports = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        query = """
        SELECT 
            gar.id, 'generated' AS report_type, gar.report_name, gar.report_description,
            gar.annotations, gar.generation_timestamp AS timestamp, hcr.target_host, 
            hcr.target_db_name, uap.profile_name, ar.rule_set_name, pt.template_name
        FROM generated_ai_reports gar
        JOIN health_check_runs hcr ON gar.run_id = hcr.id
        LEFT JOIN user_ai_profiles uap ON gar.ai_profile_id = uap.id
        LEFT JOIN analysis_rules ar ON gar.rule_set_id = ar.id
        LEFT JOIN prompt_templates pt ON gar.template_id = pt.id
        WHERE gar.generated_by_user_id = %(user_id)s
        UNION ALL
        SELECT
            ur.id, 'uploaded' AS report_type, ur.report_name, ur.report_description,
            NULL AS annotations, ur.upload_timestamp AS timestamp, 'N/A' AS target_host,
            'N/A' AS target_db_name, 'Manual Upload' AS profile_name,
            NULL AS rule_set_name, NULL AS template_name
        FROM uploaded_reports ur
        WHERE ur.uploaded_by_user_id = %(user_id)s
        ORDER BY timestamp DESC;
        """
        cursor.execute(query, {'user_id': current_user.id})
        for row in cursor.fetchall():
            reports.append({
                "id": row[0], "type": row[1], "name": row[2], "description": row[3],
                "annotations": row[4], "timestamp": row[5].isoformat(),
                "target_host": row[6], "db_name": row[7],
                "profile_name": row[8], "rules_name": row[9], "template_name": row[10]
            })
    except psycopg2.Error as e:
        current_app.logger.error(f"DB error fetching all reports: {e}")
        return jsonify({"error": "Could not fetch report history."}), 500
    finally:
        if conn: conn.close()
    return jsonify(reports)

@bp.route('/api/download-report/<int:report_id>')
@login_required
def download_report(report_id):
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT pgp_sym_decrypt(report_content::bytea, get_encryption_key()), run_id FROM generated_ai_reports WHERE id = %s AND generated_by_user_id = %s;",
            (report_id, current_user.id)
        )
        report_data = cursor.fetchone()
        if not report_data:
            abort(404, "Report not found or permission denied.")
        report_content, run_id = report_data
        return send_file(io.BytesIO(report_content.encode('utf-8')), mimetype='text/plain', as_attachment=True, download_name=f"ai_report_run_{run_id}_saved.adoc")
    except psycopg2.Error as e:
        current_app.logger.error(f"DB error downloading report: {e}")
        abort(500, "DB error occurred.")
    finally:
        if conn: conn.close()

@bp.route('/api/generated-reports/<int:report_id>', methods=['PUT'])
@login_required
def update_report_metadata(report_id):
    data = request.get_json()
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE generated_ai_reports SET report_name = %s, report_description = %s, annotations = %s WHERE id = %s AND generated_by_user_id = %s;",
            (data.get('report_name'), data.get('report_description'), data.get('annotations'), report_id, current_user.id)
        )
        conn.commit()
        if cursor.rowcount == 0:
            return jsonify({"error": "Report not found or permission denied."}), 404
        return jsonify({"status": "success"})
    except psycopg2.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"DB error updating report metadata: {e}")
        return jsonify({"error": "DB error occurred."}), 500
    finally:
        if conn: conn.close()
