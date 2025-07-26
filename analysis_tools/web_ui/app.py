from flask import Flask, render_template, request, redirect, url_for, flash, abort
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import sys
from pathlib import Path
import psycopg2
import yaml
import json
from deepdiff import DeepDiff
import re
from datetime import datetime

# Add the root directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

app = Flask(__name__)
app.config['SECRET_KEY'] = 'a-super-secret-key-that-should-be-changed'

# --- Self-contained config loader ---
def load_trends_config(config_path='config/trends.yaml'):
    try:
        project_root = Path(__file__).parent.parent.parent
        with open(project_root / config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        app.logger.error(f"Error loading trends.yaml: {e}")
        return None

# --- Flask-Login & User Model ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, user_id, username, company_id, password_change_required):
        self.id = user_id
        self.username = username
        self.company_id = company_id
        self.password_change_required = password_change_required

@login_manager.user_loader
def load_user(user_id):
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, company_id, password_change_required FROM users WHERE id = %s;", (user_id,))
        user_data = cursor.fetchone()
        if user_data:
            return User(user_id=user_data[0], username=user_data[1], company_id=user_data[2], password_change_required=user_data[3])
    except psycopg2.Error as e:
        app.logger.error(f"Database error loading user: {e}")
    finally:
        if conn: conn.close()
    return None

@app.before_request
def before_request_callback():
    if current_user.is_authenticated and current_user.password_change_required:
        if request.endpoint and request.endpoint not in ('change_password', 'logout', 'static'):
            return redirect(url_for('change_password'))

# --- Helper Functions ---
def format_path(path):
    return re.sub(r"root\['(.*?)'\]", r'\1', path).replace("'", "")

@app.template_filter('format_path')
def jinja_format_path(path):
    return format_path(path)

def fetch_runs_by_ids(db_config, run_ids):
    conn = None
    runs = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        query = "SELECT run_timestamp, findings, target_host, target_port, target_db_name FROM health_check_runs WHERE id IN %s;"
        cursor.execute(query, (tuple(run_ids),))
        rows = cursor.fetchall()
        for row in rows:
            run_timestamp, findings_json, r_host, r_port, r_dbname = row
            runs.append({"run_timestamp": run_timestamp.isoformat(), "findings": findings_json, "target_host": r_host, "target_port": r_port, "target_db_name": r_dbname})
    except psycopg2.Error as e:
        app.logger.error(f"Database error fetching runs by IDs: {e}")
    finally:
        if conn: conn.close()
    return sorted(runs, key=lambda x: x['run_timestamp'], reverse=True)

# --- Routes ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    # ... (login logic remains the same)
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    # ... (change_password logic remains the same)
    return render_template('change_password.html')

@app.route('/api/runs')
@login_required
def get_all_runs():
    """Corrected: Returns a JSON list of ALL runs for the user's company."""
    config = load_trends_config()
    db_settings = config.get('database')
    company_id = current_user.company_id
    conn = None
    all_runs = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        query = "SELECT id, run_timestamp, target_host, target_port, target_db_name FROM health_check_runs WHERE company_id = %s ORDER BY run_timestamp DESC;"
        cursor.execute(query, (company_id,))
        for row in cursor.fetchall():
            all_runs.append({
                "id": row[0],
                "timestamp": row[1].isoformat(),
                "target": f"{row[2]}:{row[3]} ({row[4]})"
            })
    except psycopg2.Error as e:
        app.logger.error(f"Database error fetching all runs: {e}")
    finally:
        if conn: conn.close()
    # Corrected: Use Flask's jsonify for proper content type
    from flask import jsonify
    return jsonify(all_runs)

@app.route('/')
@login_required
def dashboard():
    config = load_trends_config()
    db_settings = config.get('database')
    
    run_id_1 = request.args.get('run1', type=int)
    run_id_2 = request.args.get('run2', type=int)
    
    runs = []
    if run_id_1 and run_id_2:
        runs = fetch_runs_by_ids(db_settings, [run_id_1, run_id_2])
    
    changes_by_check = {}
    if len(runs) >= 2:
        diff = DeepDiff(runs[1]['findings'], runs[0]['findings'], ignore_order=True, view='tree')
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

    return render_template('dashboard.html', runs=runs, changes_by_check=changes_by_check)

if __name__ == '__main__':
    app.run(debug=True, port=5001)
