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
# IMPORTANT: Change this secret key in a production environment!
app.config['SECRET_KEY'] = 'a-super-secret-key-that-should-be-changed'

# --- Self-contained config loader ---
def load_trends_config(config_path='config/trends.yaml'):
    """Loads the trend shipper configuration for the web app."""
    try:
        project_root = Path(__file__).parent.parent.parent
        with open(project_root / config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        app.logger.error(f"Error loading trends.yaml: {e}")
        return None

# --- New: Function to check database connectivity ---
def check_db_connection():
    """Checks if a connection can be made to the database."""
    config = load_trends_config()
    if not config or 'database' not in config:
        return False, "trends.yaml not found or is missing 'database' section."
    
    db_config = config['database']
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        return True, "Successfully connected to the database."
    except psycopg2.Error as e:
        # Return a user-friendly part of the error message
        return False, f"Database connection failed: {str(e).splitlines()[0]}"
    finally:
        if conn:
            conn.close()

# --- Flask-Login Setup & User Model ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message_category = 'info'

class User(UserMixin):
    def __init__(self, user_id, username, company_id, password_change_required):
        self.id = user_id
        self.username = username
        self.company_id = company_id
        self.password_change_required = password_change_required

@login_manager.user_loader
def load_user(user_id):
    config = load_trends_config()
    if not config: return None
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
            flash("Please update your password before continuing.", "warning")
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
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        config = load_trends_config()
        if not config:
            flash("Server configuration error.", "danger")
            db_connected, db_message = check_db_connection()
            return render_template('login.html', db_connected=db_connected, db_message=db_message)

        db_config = config.get('database')
        conn = None
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute("SELECT id, password_hash FROM users WHERE username = %s;", (username,))
            user_data = cursor.fetchone()

            if user_data and check_password_hash(user_data[1], password):
                user = load_user(user_data[0])
                if user:
                    login_user(user)
                    return redirect(url_for('dashboard'))
            
            flash('Invalid username or password.', 'danger')

        except psycopg2.Error as e:
            flash(f"Database error during login.", "danger")
            app.logger.error(e)
        finally:
            if conn: conn.close()

    db_connected, db_message = check_db_connection()
    return render_template('login.html', db_connected=db_connected, db_message=db_message)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    if request.method == 'POST':
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')

        if not new_password or new_password != confirm_password:
            flash("Passwords do not match.", "danger")
            return redirect(url_for('change_password'))
        
        new_password_hash = generate_password_hash(new_password)
        config = load_trends_config()
        db_config = config.get('database')
        conn = None
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET password_hash = %s, password_change_required = FALSE WHERE id = %s;", (new_password_hash, current_user.id))
            conn.commit()
            
            flash("Password updated successfully. Please log in again.", "success")
            return redirect(url_for('logout'))
        except psycopg2.Error as e:
            flash("Database error while updating password.", "danger")
            app.logger.error(e)
        finally:
            if conn: conn.close()
    
    return render_template('change_password.html')

@app.route('/api/runs')
@login_required
def get_all_runs():
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
