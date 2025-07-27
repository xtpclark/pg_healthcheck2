from flask import Blueprint, render_template, request, jsonify, current_app
from flask_login import login_required, current_user
import psycopg2
from .database import get_unique_targets, load_user_preferences, fetch_runs_by_ids, save_user_preference
from .utils import load_trends_config, format_path
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

@bp.route('/api/runs')
@login_required
def get_all_runs():
    """API endpoint to fetch runs, with support for filtering."""
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
        
        params = {'company_ids': accessible_company_ids}
        query_parts = [
            "SELECT hcr.id, hcr.run_timestamp, hcr.target_host, hcr.target_port, hcr.target_db_name",
            "FROM health_check_runs hcr",
            "JOIN companies c ON hcr.company_id = c.id",
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
            try:
                if 'T' not in start_time_str and ' ' not in start_time_str:
                    start_time_str += 'T00:00:00'
                start_time = datetime.fromisoformat(start_time_str)
                query_parts.append("AND hcr.run_timestamp >= %(start_time)s")
                params['start_time'] = start_time
            except ValueError:
                current_app.logger.warning(f"Invalid start_time format: {start_time_str}")

        if end_time_str:
            try:
                if 'T' not in end_time_str and ' ' not in end_time_str:
                    end_time_str += 'T23:59:59'
                end_time = datetime.fromisoformat(end_time_str)
                query_parts.append("AND hcr.run_timestamp <= %(end_time)s")
                params['end_time'] = end_time
            except ValueError:
                current_app.logger.warning(f"Invalid end_time format: {end_time_str}")

        query_parts.append("ORDER BY hcr.run_timestamp DESC;")
        
        query = " ".join(query_parts)
        cursor.execute(query, params)

        for row in cursor.fetchall():
            all_runs.append({
                "id": row[0],
                "timestamp": row[1].isoformat(),
                "target": f"{row[2]}:{row[3]} ({row[4]})"
            })
    except psycopg2.Error as e:
        current_app.logger.error(f"Database error fetching all runs with filters: {e}")
    finally:
        if conn: conn.close()
    return jsonify(all_runs)

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
