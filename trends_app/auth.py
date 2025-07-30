from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash, generate_password_hash
import psycopg2
from .database import check_db_connection, load_user
from .utils import load_trends_config

bp = Blueprint('auth', __name__)

@bp.route('/login', methods=['GET', 'POST'])
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

            cursor.execute("SELECT usercanlogin(%s);", (username,))
            can_login_result = cursor.fetchone()
            can_login = can_login_result[0] if can_login_result else False

            if not can_login:
                flash('User is not active or not authorized to log in.', 'danger')
                db_connected, db_message = check_db_connection()
                return render_template('login.html', db_connected=db_connected, db_message=db_message)

            cursor.execute("SELECT id, password_hash FROM users WHERE username = %s;", (username,))
            user_data = cursor.fetchone()
            if user_data and check_password_hash(user_data[1], password):
                user = load_user(db_config, user_data[0])
                if user:
                    login_user(user)
                    return redirect(url_for('main.dashboard'))
            
            flash('Invalid username or password.', 'danger')
        except psycopg2.Error as e:
            flash(f"Database error during login.", "danger")
        finally:
            if conn: conn.close()

    db_connected, db_message = check_db_connection()
    return render_template('login.html', db_connected=db_connected, db_message=db_message)

@bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))

@bp.route('/change-password', methods=['GET', 'POST'])
@login_required
def change_password():
    if request.method == 'POST':
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')
        if not new_password or new_password != confirm_password:
            flash("Passwords do not match.", "danger")
            return redirect(url_for('auth.change_password'))
        
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
            return redirect(url_for('auth.logout'))
        except psycopg2.Error as e:
            flash("Database error while updating password.", "danger")
        finally:
            if conn: conn.close()
    return render_template('change_password.html')
