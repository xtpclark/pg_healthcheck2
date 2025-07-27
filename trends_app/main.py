from flask import Blueprint, render_template, request, jsonify, current_app, redirect, url_for, abort, send_file
from flask_login import login_required, current_user
import psycopg2
import io
from .database import get_unique_targets, load_user_preferences, fetch_runs_by_ids, save_user_preference
from .utils import load_trends_config, format_path
from .ai_connector import get_ai_recommendation
from .prompt_generator import generate_web_prompt
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

# --- Existing API Routes ---

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
    """Toggles the favorite status of a given run for the current user."""
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
    """API endpoint to save a user's filter preference."""
    data = request.get_json()
    pref_name = data.get('name')
    pref_value = data.get('value')

    if not pref_name or pref_value is None:
        return jsonify({'status': 'error', 'message': 'Missing preference name or value'}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    save_user_preference(db_settings, current_user.username, pref_name, pref_value)
    
    return jsonify({'status': 'success', 'message': f'Preference {pref_name} saved.'})

# --- NEW API ROUTES FOR AI REPORTING ---

@bp.route('/api/user-ai-profiles')
@login_required
def get_user_ai_profiles():
    """Fetches the current user's saved AI profiles."""
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
    """Fetches available analysis rule sets."""
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
    """Fetches available prompt templates."""
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    templates = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        cursor.execute("SELECT id, template_name FROM prompt_templates ORDER BY template_name;")
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
    """Generates an AI report, stores it, and returns it for download."""
    if not current_user.has_privilege('GenerateReports'):
        abort(403)

    data = request.get_json()
    run_id = data.get('run_id')
    profile_id = data.get('profile_id')
    template_id = data.get('template_id')
    rule_set_id = data.get('rule_set_id')
    report_name = data.get('report_name')
    report_description = data.get('report_description')

    if not all([run_id, profile_id, template_id, rule_set_id]):
        return jsonify({"error": "Missing required parameters."}), 400

    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()

        # 1. Fetch the findings JSON for the selected run
        cursor.execute("SELECT findings FROM health_check_runs WHERE id = %s;", (run_id,))
        findings_row = cursor.fetchone()
        if not findings_row:
            return jsonify({"error": "Run not found."}), 404
        findings_json = findings_row[0]

        # 2. Generate the prompt
        prompt = generate_web_prompt(findings_json, rule_set_id, template_id)
        if prompt.startswith("Error:"):
            return jsonify({"error": prompt}), 500

        # 3. Get the AI recommendation
        ai_response = get_ai_recommendation(prompt, profile_id)
        if ai_response.startswith("Error:"):
            return jsonify({"error": ai_response}), 500

        # 4. Encrypt and store the report
        cursor.execute(
            """
            INSERT INTO generated_ai_reports (
                run_id, rule_set_id, ai_profile_id, generated_by_user_id,
                report_name, report_description, report_content
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                pgp_sym_encrypt(%s, get_encryption_key())
            );
            """,
            (run_id, rule_set_id, profile_id, current_user.id, report_name, report_description, ai_response)
        )
        conn.commit()

        # 5. Return the report for download
        return send_file(
            io.BytesIO(ai_response.encode('utf-8')),
            mimetype='text/plain',
            as_attachment=True,
            download_name=f"ai_report_run_{run_id}.adoc"
        )

    except psycopg2.Error as e:
        if conn: conn.rollback()
        current_app.logger.error(f"Database error during AI report generation: {e}")
        return jsonify({"error": "A database error occurred."}), 500
    finally:
        if conn: conn.close()

# --- Placeholder Routes for Report History ---
@bp.route('/report-history')
@login_required
def report_history():
    # This will render the new report history page
    return "Report History Page (to be implemented)"

@bp.route('/api/generated-reports')
@login_required
def get_generated_reports():
    # This will fetch the list of reports for the history page
    return jsonify([])

@bp.route('/api/download-report/<int:report_id>')
@login_required
def download_report(report_id):
    # This will decrypt and send a stored report
    return "Download report endpoint (to be implemented)"

@bp.route('/api/generated-reports/<int:report_id>', methods=['PUT'])
@login_required
def update_report_metadata(report_id):
    # This will update the name, description, etc. of a report
    return jsonify({"status": "success"})
