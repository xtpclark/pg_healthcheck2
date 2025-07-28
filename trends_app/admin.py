from flask import Blueprint, render_template, request, redirect, url_for, flash, abort, jsonify
from flask_login import login_required, current_user
from werkzeug.security import generate_password_hash
import psycopg2
from .database import load_user
from .utils import load_trends_config

# The url_prefix makes all routes in this file start with /admin
bp = Blueprint('admin', __name__, url_prefix='/admin')

# --- USER MANAGEMENT ROUTES ---

@bp.route('/users')
@login_required
def list_users():
    """Admin page to list all users."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)
    
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    all_users = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM users ORDER BY username;")
        user_ids = [row[0] for row in cursor.fetchall()]
        all_users = [load_user(db_config, user_id) for user_id in user_ids]
    except psycopg2.Error as e:
        flash("Database error while fetching users.", "danger")
    finally:
        if conn: conn.close()
    
    return render_template('admin/users.html', users=all_users)

@bp.route('/users/create', methods=['GET', 'POST'])
@login_required
def create_user():
    """Admin page to create a new user and assign them to groups (roles)."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)
        
    config = load_trends_config()
    db_config = config.get('database')
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        is_admin = 'is_admin' in request.form
        company_ids = request.form.getlist('companies', type=int)
        group_ids = request.form.getlist('groups', type=int)
        password_hash = generate_password_hash(password)
        
        conn = None
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO users (username, password_hash, is_admin) VALUES (%s, %s, %s) RETURNING id;", (username, password_hash, is_admin))
            new_user_id = cursor.fetchone()[0]

            for company_id in company_ids:
                cursor.execute("INSERT INTO user_company_access (user_id, company_id) VALUES (%s, %s);", (new_user_id, company_id))

            for group_id in group_ids:
                 cursor.execute("INSERT INTO usrgrp (usrgrp_grp_id, usrgrp_username) VALUES (%s, %s);", (group_id, username))

            # --- NEW: Set the 'active' preference so the new user can log in ---
            cursor.execute("SELECT setuserpreference(%s, 'active', 't');", (username,))

            conn.commit()
            flash(f"User '{username}' created successfully.", "success")
            return redirect(url_for('admin.list_users'))
        except psycopg2.Error as e:
            flash(f"Database error: {e}", "danger")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    # GET Request: Fetch companies and groups for the form
    all_companies = []
    all_groups = []
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT id, company_name FROM companies ORDER BY company_name;")
        all_companies = [{"id": row[0], "company_name": row[1]} for row in cursor.fetchall()]

        cursor.execute("SELECT grp_id, grp_name FROM grp ORDER BY grp_name;")
        all_groups = [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]
    except psycopg2.Error as e:
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()
        
    return render_template('admin/user_form.html', all_companies=all_companies, all_groups=all_groups, user=None)


@bp.route('/users/edit/<int:user_id>', methods=['GET', 'POST'])
@login_required
def edit_user(user_id):
    """Admin page to edit an existing user's groups (roles)."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    user_to_edit = load_user(db_config, user_id)
    if not user_to_edit:
        flash("User not found.", "danger")
        return redirect(url_for('admin.list_users'))

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        is_admin = 'is_admin' in request.form
        company_ids = request.form.getlist('companies', type=int)
        group_ids = request.form.getlist('groups', type=int)
        
        conn = None
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET username = %s, is_admin = %s WHERE id = %s;", (username, is_admin, user_id))

            if password:
                password_hash = generate_password_hash(password)
                cursor.execute("UPDATE users SET password_hash = %s WHERE id = %s;", (password_hash, user_id))

            cursor.execute("DELETE FROM user_company_access WHERE user_id = %s;", (user_id,))
            for company_id in company_ids:
                cursor.execute("INSERT INTO user_company_access (user_id, company_id) VALUES (%s, %s);", (user_id, company_id))
            
            cursor.execute("DELETE FROM usrgrp WHERE usrgrp_username = %s;", (user_to_edit.username,))
            for group_id in group_ids:
                cursor.execute("INSERT INTO usrgrp (usrgrp_grp_id, usrgrp_username) VALUES (%s, %s);", (group_id, username))

            conn.commit()
            flash(f"User '{username}' updated successfully.", "success")
            return redirect(url_for('admin.list_users'))

        except psycopg2.Error as e:
            flash(f"Database error: {e}", "danger")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    # GET Request: Fetch all necessary data for the form
    all_companies = []
    all_groups = []
    user_company_ids = []
    user_group_ids = []
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, company_name FROM companies ORDER BY company_name;")
        all_companies = [{"id": row[0], "company_name": row[1]} for row in cursor.fetchall()]
        
        cursor.execute("SELECT grp_id, grp_name FROM grp ORDER BY grp_name;")
        all_groups = [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]
        
        user_company_ids = [c['id'] for c in user_to_edit.accessible_companies]
        
        cursor.execute("SELECT usrgrp_grp_id FROM usrgrp WHERE usrgrp_username = %s;", (user_to_edit.username,))
        user_group_ids = [row[0] for row in cursor.fetchall()]

    except psycopg2.Error as e:
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()
    
    return render_template('admin/user_form.html', 
                           user=user_to_edit, 
                           all_companies=all_companies, 
                           all_groups=all_groups,
                           user_company_ids=user_company_ids,
                           user_group_ids=user_group_ids)


# --- ROLE (GROUP) MANAGEMENT ROUTES ---

@bp.route('/roles')
@login_required
def list_roles():
    """Admin page to list all roles (groups)."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)
    
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    roles = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT g.grp_id, g.grp_name, g.grp_descrip, COUNT(ug.usrgrp_id)
            FROM grp g
            LEFT JOIN usrgrp ug ON g.grp_id = ug.usrgrp_grp_id
            GROUP BY g.grp_id, g.grp_name, g.grp_descrip
            ORDER BY g.grp_name;
        """)
        roles = [{
            "id": row[0], "name": row[1], "description": row[2], "user_count": row[3]
        } for row in cursor.fetchall()]
    except psycopg2.Error as e:
        flash(f"Database error fetching roles: {e}", "danger")
    finally:
        if conn: conn.close()

    return render_template('admin/roles.html', roles=roles)


@bp.route('/roles/create', methods=['GET', 'POST'])
@login_required
def create_role():
    """Admin page to create a new role and assign privileges."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')

    if request.method == 'POST':
        name = request.form.get('name')
        description = request.form.get('description')
        privilege_ids = request.form.getlist('privileges', type=int)
        conn = None
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO grp (grp_name, grp_descrip) VALUES (%s, %s) RETURNING grp_id;", (name, description))
            new_role_id = cursor.fetchone()[0]

            for priv_id in privilege_ids:
                cursor.execute("INSERT INTO grppriv (grppriv_grp_id, grppriv_priv_id) VALUES (%s, %s);", (new_role_id, priv_id))

            conn.commit()
            flash(f"Role '{name}' created successfully.", "success")
            return redirect(url_for('admin.list_roles'))
        except psycopg2.Error as e:
            if conn: conn.rollback()
            flash(f"Database error: {e}", "danger")
        finally:
            if conn: conn.close()

    # GET Request: Fetch privileges for the form
    all_privileges = []
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT priv_id, priv_name FROM priv ORDER BY priv_name;")
        all_privileges = [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]
    except psycopg2.Error as e:
        flash(f"Database error fetching privileges: {e}", "danger")
    finally:
        if conn: conn.close()

    return render_template('admin/role_form.html', all_privileges=all_privileges, role=None, role_priv_ids=[])

@bp.route('/roles/edit/<int:role_id>', methods=['GET', 'POST'])
@login_required
def edit_role(role_id):
    """Admin page to edit an existing role's privileges."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)
    
    config = load_trends_config()
    db_config = config.get('database')
    
    if request.method == 'POST':
        name = request.form.get('name')
        description = request.form.get('description')
        privilege_ids = request.form.getlist('privileges', type=int)
        conn = None
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute("UPDATE grp SET grp_name = %s, grp_descrip = %s WHERE grp_id = %s;", (name, description, role_id))
            
            cursor.execute("DELETE FROM grppriv WHERE grppriv_grp_id = %s;", (role_id,))
            for priv_id in privilege_ids:
                cursor.execute("INSERT INTO grppriv (grppriv_grp_id, grppriv_priv_id) VALUES (%s, %s);", (role_id, priv_id))

            conn.commit()
            flash(f"Role '{name}' updated successfully.", "success")
            return redirect(url_for('admin.list_roles'))
        except psycopg2.Error as e:
            if conn: conn.rollback()
            flash(f"Database error: {e}", "danger")
        finally:
            if conn: conn.close()

    # GET Request: Fetch data for the form
    role = {}
    all_privileges = []
    role_priv_ids = []
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT grp_id, grp_name, grp_descrip FROM grp WHERE grp_id = %s;", (role_id,))
        role_data = cursor.fetchone()
        if not role_data:
            flash("Role not found.", "danger")
            return redirect(url_for('admin.list_roles'))
        role = {"id": role_data[0], "name": role_data[1], "description": role_data[2]}

        cursor.execute("SELECT priv_id, priv_name FROM priv ORDER BY priv_name;")
        all_privileges = [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]

        cursor.execute("SELECT grppriv_priv_id FROM grppriv WHERE grppriv_grp_id = %s;", (role_id,))
        role_priv_ids = [row[0] for row in cursor.fetchall()]

    except psycopg2.Error as e:
        flash(f"Database error fetching data for role edit: {e}", "danger")
    finally:
        if conn: conn.close()
        
    return render_template('admin/role_form.html', role=role, all_privileges=all_privileges, role_priv_ids=role_priv_ids)


# --- AI PROVIDER MANAGEMENT ROUTES (Unchanged) ---

@bp.route('/ai-providers')
@login_required
def list_ai_providers():
    if not current_user.is_admin:
        abort(403)
    
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    providers = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT id, provider_name, api_model, is_active, allow_user_keys FROM ai_providers ORDER BY provider_name;")
        for row in cursor.fetchall():
            providers.append({
                "id": row[0], "name": row[1], "model": row[2],
                "is_active": row[3], "allow_user_keys": row[4]
            })
    except psycopg2.Error as e:
        flash("Database error while fetching AI providers.", "danger")
    finally:
        if conn: conn.close()
    
    return render_template('admin/ai_providers.html', providers=providers)

@bp.route('/ai-providers/create', methods=['POST'])
@login_required
def create_ai_provider():
    if not current_user.is_admin:
        abort(403)
        
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        provider_name = request.form.get('provider_name')
        api_endpoint = request.form.get('api_endpoint')
        api_model = request.form.get('api_model')
        api_key = request.form.get('api_key', '')
        is_active = 'is_active' in request.form
        allow_user_keys = 'allow_user_keys' in request.form

        if not all([provider_name, api_endpoint, api_model]):
            flash("Provider Name, API Endpoint, and Model are required.", "danger")
            return redirect(url_for('admin.list_ai_providers'))

        encrypted_key = None
        if api_key:
            cursor.execute("SELECT pgp_sym_encrypt(%s, get_encryption_key());", (api_key,))
            encrypted_key = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO ai_providers (
                provider_name, api_endpoint, api_model, encrypted_api_key,
                is_active, allow_user_keys
            ) VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (provider_name, api_endpoint, api_model, encrypted_key, is_active, allow_user_keys)
        )
        conn.commit()
        flash(f"AI Provider '{provider_name}' created successfully.", "success")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()
        
    return redirect(url_for('admin.list_ai_providers'))

@bp.route('/ai-providers/delete/<int:provider_id>', methods=['POST'])
@login_required
def delete_ai_provider(provider_id):
    if not current_user.is_admin:
        abort(403)
        
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM ai_providers WHERE id = %s;", (provider_id,))
        conn.commit()
        flash("AI Provider deleted successfully.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash("Database error while deleting provider.", "danger")
    finally:
        if conn: conn.close()
        
    return redirect(url_for('admin.list_ai_providers'))


# --- PRIVILEGE MANAGEMENT ROUTES ---

@bp.route('/privileges')
@login_required
def list_privileges():
    """Admin page to list all privileges."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    privileges = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT priv_id, priv_module, priv_name, priv_descrip FROM priv ORDER BY priv_module, priv_name;")
        privileges = [{
            "id": row[0], "module": row[1], "name": row[2], "description": row[3]
        } for row in cursor.fetchall()]
    except psycopg2.Error as e:
        flash(f"Database error fetching privileges: {e}", "danger")
    finally:
        if conn: conn.close()

    return render_template('admin/privileges.html', privileges=privileges)


@bp.route('/privileges/upsert', methods=['POST'])
@login_required
def upsert_privilege():
    """Handles creating or updating a privilege by calling the createpriv function."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    
    module = request.form.get('module')
    name = request.form.get('name')
    description = request.form.get('description')
    
    if not all([module, name, description]):
        flash("Module, Name, and Description are required.", "danger")
        return redirect(url_for('admin.list_privileges'))

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        # Call the database function to perform the upsert
        cursor.execute("SELECT createpriv(%s, %s, %s);", (module, name, description))
        conn.commit()
        flash(f"Privilege '{name}' saved successfully.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error saving privilege: {e}", "danger")
    finally:
        if conn: conn.close()

    return redirect(url_for('admin.list_privileges'))
