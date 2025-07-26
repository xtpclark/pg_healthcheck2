from flask import Flask, render_template, request, redirect, url_for, flash, abort, jsonify
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import sys
from pathlib import Path
import psycopg2
import yaml
import json
from deepdiff import DeepDiff
import re

# Add the root directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

app = Flask(__name__)
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

# --- Restored: Function to check database connectivity ---
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
        return False, f"Database connection failed: {str(e).splitlines()[0]}"
    finally:
        if conn:
            conn.close()

# --- Flask-Login Setup & User Model ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    """Updated User model for advanced privilege management."""
    def __init__(self, user_id, username, is_admin, password_change_required, accessible_companies=None, privileges=None):
        self.id = user_id
        self.username = username
        self.is_admin = is_admin
        self.password_change_required = password_change_required
        self.accessible_companies = accessible_companies or []
        self.privileges = privileges or set()

    def has_privilege(self, privilege_name):
        """Checks if a user has a specific privilege."""
        return self.is_admin or privilege_name in self.privileges

@login_manager.user_loader
def load_user(user_id):
    """Loads a user and their associated companies and privileges."""
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, is_admin, password_change_required FROM users WHERE id = %s;", (user_id,))
        user_data = cursor.fetchone()
        if user_data:
            user_id, username, is_admin, password_change_required = user_data
            
            cursor.execute("SELECT c.id, c.company_name FROM user_company_access uca JOIN companies c ON uca.company_id = c.id WHERE uca.user_id = %s;", (user_id,))
            accessible_companies = [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]

            cursor.execute("SELECT p.priv_name FROM usrpriv up JOIN priv p ON up.usrpriv_priv_id = p.priv_id WHERE up.usrpriv_username = %s;", (username,))
            privileges = {row[0] for row in cursor.fetchall()}

            return User(user_id, username, is_admin, password_change_required, accessible_companies, privileges)
    except psycopg2.Error as e:
        app.logger.error(f"Database error loading user: {e}")
    finally:
        if conn: conn.close()
    return None

# --- Middleware and Helper Functions ---
@app.before_request
def before_request_callback():
    if current_user.is_authenticated and current_user.password_change_required:
        if request.endpoint and request.endpoint not in ('change_password', 'logout', 'static'):
            return redirect(url_for('change_password'))

def format_path(path):
    return re.sub(r"root\['(.*?)'\]", r'\1', path).replace("'", "")

@app.template_filter('format_path')
def jinja_format_path(path):
    return format_path(path)

def fetch_runs_by_ids(db_config, run_ids, accessible_company_ids):
    """Fetches specific runs, ensuring they belong to the user's accessible companies."""
    conn = None
    runs = []
    if not accessible_company_ids:
        return runs
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        query = "SELECT run_timestamp, findings, target_host, target_port, target_db_name FROM health_check_runs WHERE id IN %s AND company_id = ANY(%s);"
        cursor.execute(query, (tuple(run_ids), accessible_company_ids))
        rows = cursor.fetchall()
        for row in rows:
            run_timestamp, findings_json, r_host, r_port, r_dbname = row
            runs.append({"run_timestamp": run_timestamp.isoformat(), "findings": findings_json, "target_host": r_host, "target_port": r_port, "target_db_name": r_dbname})
    except psycopg2.Error as e:
        app.logger.error(f"Database error fetching runs by IDs: {e}")
    finally:
        if conn: conn.close()
    return sorted(runs, key=lambda x: x['run_timestamp'], reverse=True)

# --- Main Application Routes ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        config = load_trends_config()
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

    # Call the connection check for GET requests and failed POSTs
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
    """API endpoint to fetch all runs for the user's accessible companies."""
    config = load_trends_config()
    db_settings = config.get('database')
    
    accessible_company_ids = [c['id'] for c in current_user.accessible_companies]
    if not accessible_company_ids:
        return jsonify([])

    conn = None
    all_runs = []
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        query = "SELECT id, run_timestamp, target_host, target_port, target_db_name FROM health_check_runs WHERE company_id = ANY(%s) ORDER BY run_timestamp DESC;"
        cursor.execute(query, (accessible_company_ids,))
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
    return jsonify(all_runs)

@app.route('/')
@login_required
def dashboard():
    """Main dashboard route. Updated for multi-company access."""
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

# --- Admin Routes ---
@app.route('/admin/users')
@login_required
def admin_list_users():
    """Admin page to list all users."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403) # Forbidden
    
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    all_users = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        # Fetch all users. A more complex query could join to get company names directly.
        cursor.execute("SELECT id FROM users ORDER BY username;")
        user_ids = [row[0] for row in cursor.fetchall()]
        
        # Use our existing user_loader to get fully populated user objects
        all_users = [load_user(user_id) for user_id in user_ids]

    except psycopg2.Error as e:
        flash("Database error while fetching users.", "danger")
        app.logger.error(e)
    finally:
        if conn: conn.close()
    
    return render_template('admin/users.html', users=all_users)

if __name__ == '__main__':
    app.run(debug=True, port=5001)
