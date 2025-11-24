from flask import Blueprint, render_template, request, redirect, url_for, flash, abort, jsonify, current_app, Response
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
    """Admin page to list all users and their active status."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)
    
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    users_with_status = [] # MODIFIED: Create a new list for users and their status
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        # Fetch all user data
        cursor.execute("SELECT id, username FROM users ORDER BY username;")
        users_data = cursor.fetchall()
        
        for user_id, username in users_data:
            user = load_user(db_config, user_id)
            if user:
                # Check if the user can log in to determine their active status
                cursor.execute("SELECT usercanlogin(%s);", (username,))
                is_active = cursor.fetchone()[0]
                # MODIFIED: Append a dictionary with user and status instead of modifying the user object
                users_with_status.append({'user': user, 'is_active': is_active})
                
    except psycopg2.Error as e:
        flash("Database error while fetching users.", "danger")
    finally:
        if conn: conn.close()
    
    # MODIFIED: Pass the new list to the template
    return render_template('admin/users.html', users_with_status=users_with_status)


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

            # Set the 'active' preference so the new user can log in
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


@bp.route('/users/disable/<int:user_id>', methods=['POST'])
@login_required
def disable_user(user_id):
    """Disables a user by setting their 'active' usrpref to 'f'."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    user_to_disable = load_user(db_config, user_id)

    if not user_to_disable:
        flash("User not found.", "danger")
        return redirect(url_for('admin.list_users'))

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        # Call the database function to set the 'active' preference to 'f'
        cursor.execute("SELECT setuserpreference(%s, 'active', 'f');", (user_to_disable.username,))
        conn.commit()
        flash(f"User '{user_to_disable.username}' has been disabled.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()
    
    return redirect(url_for('admin.list_users'))


@bp.route('/users/enable/<int:user_id>', methods=['POST'])
@login_required
def enable_user(user_id):
    """Enables a user by setting their 'active' usrpref to 't'."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    user_to_enable = load_user(db_config, user_id)

    if not user_to_enable:
        flash("User not found.", "danger")
        return redirect(url_for('admin.list_users'))

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        # Call the database function to set the 'active' preference to 't'
        cursor.execute("SELECT setuserpreference(%s, 'active', 't');", (user_to_enable.username,))
        conn.commit()
        flash(f"User '{user_to_enable.username}' has been enabled.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()
    
    return redirect(url_for('admin.list_users'))


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

@bp.route('/ai-providers', methods=['GET'])
@login_required
def list_ai_providers():
    """Display all AI providers for admin management."""
    # Check admin privilege
    
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    providers = []
    
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, provider_name, api_endpoint, api_model, is_active, allow_user_keys,
                   provider_type, supports_discovery, models_last_refreshed
            FROM ai_providers
            ORDER BY provider_name;
        """)
        
        for row in cursor.fetchall():
            last_refreshed = row[8].strftime('%Y-%m-%d %H:%M') if row[8] else None
            providers.append({
                'id': row[0],
                'name': row[1],
                'endpoint': row[2],
                'model': row[3],
                'is_active': row[4],
                'allow_user_keys': row[5],
                'provider_type': row[6],
                'supports_discovery': row[7],
                'last_refreshed': last_refreshed
            })
            
    except psycopg2.Error as e:
        flash("Database error loading providers.", "danger")
        current_app.logger.error(f"Error loading providers: {e}")
    finally:
        if conn:
            conn.close()
    
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


# --- NEW: TEMPLATE ASSET MANAGEMENT ROUTES ---

@bp.route('/template-assets')
@login_required
def list_template_assets():
    """Admin page to list and manage template assets."""
    if not current_user.has_privilege('AdministerUsers'): # Re-use privilege for simplicity
        abort(403)
    
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    assets = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT id, asset_name, mime_type, created_at FROM template_assets ORDER BY asset_name;")
        for row in cursor.fetchall():
            assets.append({
                "id": row[0], "name": row[1], "mime_type": row[2], "created_at": row[3]
            })
    except psycopg2.Error as e:
        flash(f"Database error fetching assets: {e}", "danger")
    finally:
        if conn: conn.close()
        
    return render_template('admin/template_assets.html', assets=assets)

@bp.route('/template-assets/upload', methods=['POST'])
@login_required
def upload_template_asset():
    """Handles the upload of a new template asset."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    asset_name = request.form.get('asset_name')
    if 'asset_file' not in request.files or not asset_name:
        flash("Asset Name and a file are required.", "danger")
        return redirect(url_for('admin.list_template_assets'))

    file = request.files['asset_file']
    if file.filename == '':
        flash("No selected file.", "danger")
        return redirect(url_for('admin.list_template_assets'))

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO template_assets (asset_name, mime_type, asset_data) VALUES (%s, %s, %s);",
            (asset_name, file.mimetype, file.read())
        )
        conn.commit()
        flash(f"Asset '{asset_name}' uploaded successfully.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()
        
    return redirect(url_for('admin.list_template_assets'))

@bp.route('/template-assets/delete/<int:asset_id>', methods=['POST'])
@login_required
def delete_template_asset(asset_id):
    """Deletes a template asset."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)
        
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM template_assets WHERE id = %s;", (asset_id,))
        conn.commit()
        flash("Asset deleted successfully.", "success")
    except psycopg2.Error as e:
        if conn: conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn: conn.close()
        
    return redirect(url_for('admin.list_template_assets'))

# This route is public so images can be rendered in templates without a login
@bp.route('/assets/<string:asset_name>')
def get_template_asset(asset_name):
    """Serves a template asset's binary data from the database."""
    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT asset_data, mime_type FROM template_assets WHERE asset_name = %s;", (asset_name,))
        asset = cursor.fetchone()
        if asset:
            return Response(asset[0], mimetype=asset[1])
        else:
            abort(404)
    except psycopg2.Error as e:
        abort(500)
    finally:
        if conn: conn.close()


# In your admin.py blueprint

@bp.route('/refresh-provider-models/<int:provider_id>', methods=['POST'])
@login_required
def refresh_provider_models(provider_id):
    """Admin route to force refresh models for a provider."""
    if not current_user.has_privilege('ManageAIProviders'):  # Or whatever admin privilege you use
        return jsonify({'error': 'Permission denied'}), 403
    
    config = load_trends_config()
    db_settings = config.get('database')
    conn = None
    
    try:
        conn = psycopg2.connect(**db_settings)
        cursor = conn.cursor()
        
        # Reset last_refreshed to force re-discovery
        cursor.execute("""
            UPDATE ai_providers 
            SET models_last_refreshed = NULL
            WHERE id = %s;
        """, (provider_id,))
        conn.commit()
        
        # Import the function from profile blueprint
        from .profile import get_provider_models as profile_get_models
        
        # Call the existing discovery function
        # Note: This assumes get_provider_models doesn't check privileges internally
        # or you might need to refactor it to a shared utility function
        return profile_get_models(provider_id)
        
    except Exception as e:
        current_app.logger.error(f"Error refreshing models: {e}")
        return jsonify({'error': 'Failed to refresh models'}), 500
    finally:
        if conn:
            conn.close()


# --- METRICS MANAGEMENT ROUTES ---

@bp.route('/metrics')
@login_required
def list_metrics():
    """Admin page to list and manage all system metrics."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    metrics = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT metric_id, metric_name, metric_value, metric_module
            FROM metric
            ORDER BY metric_module NULLS LAST, metric_name;
        """)
        metrics = [
            {
                'id': row[0],
                'name': row[1],
                'value': row[2],
                'module': row[3]
            }
            for row in cursor.fetchall()
        ]
    except psycopg2.Error as e:
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return render_template('admin/metrics.html', metrics=metrics)


@bp.route('/metrics/create', methods=['POST'])
@login_required
def create_metric():
    """Create a new system metric using setmetric() stored procedure."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    metric_name = request.form.get('metric_name', '').strip()
    metric_value = request.form.get('metric_value', '').strip()
    metric_module = request.form.get('metric_module', '').strip() or None  # Convert empty string to NULL

    if not metric_name:
        flash("Metric name is required.", "danger")
        return redirect(url_for('admin.list_metrics'))

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Check if metric already exists (setmetric will update if exists)
        cursor.execute("SELECT metric_id FROM metric WHERE metric_name = %s;", (metric_name,))
        if cursor.fetchone():
            flash(f"Metric '{metric_name}' already exists. Use edit to update it.", "danger")
            return redirect(url_for('admin.list_metrics'))

        # Use setmetric() stored procedure
        cursor.execute("SELECT setmetric(%s, %s, %s);", (metric_name, metric_value, metric_module))
        conn.commit()
        flash(f"Metric '{metric_name}' created successfully.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return redirect(url_for('admin.list_metrics'))


@bp.route('/metrics/update/<int:metric_id>', methods=['POST'])
@login_required
def update_metric(metric_id):
    """Update an existing system metric using setmetric() stored procedure."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    metric_value = request.form.get('metric_value', '').strip()
    metric_module = request.form.get('metric_module', '').strip() or None  # Convert empty string to NULL

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Get metric name from ID
        cursor.execute("SELECT metric_name FROM metric WHERE metric_id = %s;", (metric_id,))
        result = cursor.fetchone()
        if not result:
            flash("Metric not found.", "danger")
            return redirect(url_for('admin.list_metrics'))

        metric_name = result[0]

        # Use setmetric() stored procedure (upsert behavior)
        cursor.execute("SELECT setmetric(%s, %s, %s);", (metric_name, metric_value, metric_module))
        conn.commit()
        flash("Metric updated successfully.", "success")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return redirect(url_for('admin.list_metrics'))


@bp.route('/metrics/delete/<int:metric_id>', methods=['POST'])
@login_required
def delete_metric(metric_id):
    """Delete a system metric."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        # Get metric name for confirmation message
        cursor.execute("SELECT metric_name FROM metric WHERE metric_id = %s;", (metric_id,))
        result = cursor.fetchone()
        if result:
            metric_name = result[0]
            cursor.execute("DELETE FROM metric WHERE metric_id = %s;", (metric_id,))
            conn.commit()
            flash(f"Metric '{metric_name}' deleted successfully.", "success")
        else:
            flash("Metric not found.", "danger")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return redirect(url_for('admin.list_metrics'))




# --- TECHNOLOGY MANAGEMENT ROUTES ---

@bp.route('/technologies')
@login_required
def list_technologies():
    """Admin page to list and manage database technologies."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    technologies = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        # Use stored procedure to fetch all technologies
        cursor.execute("SELECT * FROM fetchtechtypes(FALSE) ORDER BY order_num, descrip")
        technologies = [
            {
                'id': row[0],
                'code': row[1],
                'descrip': row[2],
                'order_num': row[3],
                'enabled': row[4],
                'notes': row[5]
            }
            for row in cursor.fetchall()
        ]
    except psycopg2.Error as e:
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return render_template('admin/technologies.html', technologies=technologies)


@bp.route('/technologies/create', methods=['POST'])
@login_required
def create_technology():
    """Create a new technology via stored procedure."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    code = request.form.get('code', '').strip()
    descrip = request.form.get('descrip', '').strip()
    order_num = request.form.get('order_num', '999')
    enabled = request.form.get('enabled') == 'true'
    notes = request.form.get('notes', '').strip()

    if not code or not descrip:
        flash("Technology code and description are required.", "danger")
        return redirect(url_for('admin.list_technologies'))

    try:
        order_num = int(order_num)
    except ValueError:
        flash("Order must be a number.", "danger")
        return redirect(url_for('admin.list_technologies'))

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Use stored procedure to create/update technology
        cursor.execute(
            "SELECT settechtype(%s, %s, %s, %s, %s)",
            (code, descrip, order_num, enabled, notes if notes else None)
        )
        conn.commit()
        flash(f"Technology '{descrip}' created successfully.", "success")

        # Clear the models.py cache so privilege checks use new data
        from .models import clear_tech_map_cache
        clear_tech_map_cache()

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return redirect(url_for('admin.list_technologies'))


@bp.route('/technologies/update/<int:tech_id>', methods=['POST'])
@login_required
def update_technology(tech_id):
    """Update an existing technology via stored procedure."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    code = request.form.get('code', '').strip()
    descrip = request.form.get('descrip', '').strip()
    order_num = request.form.get('order_num', '999')
    enabled = request.form.get('enabled') == 'true'
    notes = request.form.get('notes', '').strip()

    if not code or not descrip:
        flash("Technology code and description are required.", "danger")
        return redirect(url_for('admin.list_technologies'))

    try:
        order_num = int(order_num)
    except ValueError:
        flash("Order must be a number.", "danger")
        return redirect(url_for('admin.list_technologies'))

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Use stored procedure (settechtype is upsert - it will update existing)
        cursor.execute(
            "SELECT settechtype(%s, %s, %s, %s, %s)",
            (code, descrip, order_num, enabled, notes if notes else None)
        )
        conn.commit()
        flash(f"Technology '{descrip}' updated successfully.", "success")

        # Clear the models.py cache
        from .models import clear_tech_map_cache
        clear_tech_map_cache()

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return redirect(url_for('admin.list_technologies'))


@bp.route('/technologies/delete/<int:tech_id>', methods=['POST'])
@login_required
def delete_technology(tech_id):
    """Delete a technology via stored procedure."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # First get the code to delete by code (stored procedure uses code, not ID)
        cursor.execute(
            "SELECT code FROM fetchtechtypes(FALSE) WHERE id = %s",
            (tech_id,)
        )
        result = cursor.fetchone()

        if result:
            tech_code = result[0]
            # Use stored procedure to delete
            cursor.execute("SELECT deletetechtype(%s)", (tech_code,))
            conn.commit()
            flash(f"Technology '{tech_code}' deleted successfully.", "success")

            # Clear the models.py cache
            from .models import clear_tech_map_cache
            clear_tech_map_cache()
        else:
            flash("Technology not found.", "danger")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return redirect(url_for('admin.list_technologies'))


# --- API KEY TYPE MANAGEMENT ROUTES ---

@bp.route('/api-key-types')
@login_required
def list_api_key_types():
    """Admin page to list and manage API key types."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    key_types = []
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        # Use stored procedure to fetch all key types
        cursor.execute("SELECT * FROM fetch_api_key_types(FALSE) ORDER BY key_type_order, key_type_name")
        key_types = [
            {
                'id': row[0],
                'code': row[1],
                'name': row[2],
                'description': row[3],
                'requires_company': row[4],
                'allows_trial_limit': row[5],
                'default_expiry_days': row[6],
                'order_num': row[7],
                'enabled': row[8],
                'notes': row[9],
                'default_max_submissions': row[10],
                'rate_limit_period': row[11],
                'rate_limit_count': row[12]
            }
            for row in cursor.fetchall()
        ]
    except psycopg2.Error as e:
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return render_template('admin/api_key_types.html', key_types=key_types)


@bp.route('/api-key-types/create', methods=['POST'])
@login_required
def create_api_key_type():
    """Create a new API key type via stored procedure."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    code = request.form.get('code', '').strip()
    name = request.form.get('name', '').strip()
    description = request.form.get('description', '').strip()
    requires_company = request.form.get('requires_company') == 'true'
    allows_trial_limit = request.form.get('allows_trial_limit') == 'true'
    default_expiry_days = request.form.get('default_expiry_days', '').strip()
    order_num = request.form.get('order_num', '999')
    enabled = request.form.get('enabled') == 'true'
    notes = request.form.get('notes', '').strip()
    default_max_submissions = request.form.get('default_max_submissions', '').strip()
    rate_limit_period = request.form.get('rate_limit_period', '').strip() or None
    rate_limit_count = request.form.get('rate_limit_count', '').strip()

    if not code or not name:
        flash("Key type code and name are required.", "danger")
        return redirect(url_for('admin.list_api_key_types'))

    try:
        order_num = int(order_num)
    except ValueError:
        flash("Order must be a number.", "danger")
        return redirect(url_for('admin.list_api_key_types'))

    # Convert default_expiry_days to integer or None
    if default_expiry_days:
        try:
            default_expiry_days = int(default_expiry_days)
        except ValueError:
            flash("Default expiry days must be a number.", "danger")
            return redirect(url_for('admin.list_api_key_types'))
    else:
        default_expiry_days = None

    # Convert default_max_submissions to integer or None
    if default_max_submissions:
        try:
            default_max_submissions = int(default_max_submissions)
        except ValueError:
            flash("Default max submissions must be a number.", "danger")
            return redirect(url_for('admin.list_api_key_types'))
    else:
        default_max_submissions = None

    # Convert rate_limit_count to integer or None
    if rate_limit_count:
        try:
            rate_limit_count = int(rate_limit_count)
        except ValueError:
            flash("Rate limit count must be a number.", "danger")
            return redirect(url_for('admin.list_api_key_types'))
    else:
        rate_limit_count = None

    # Validate rate limit consistency
    if (rate_limit_period and not rate_limit_count) or (not rate_limit_period and rate_limit_count):
        flash("Rate limit period and count must both be set or both be empty.", "danger")
        return redirect(url_for('admin.list_api_key_types'))

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Use stored procedure to create/update key type
        cursor.execute(
            "SELECT set_api_key_type(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (code, name, description, requires_company, allows_trial_limit,
             default_expiry_days, order_num, enabled, notes if notes else None,
             default_max_submissions, rate_limit_period, rate_limit_count)
        )
        conn.commit()
        flash(f"API key type '{name}' created successfully.", "success")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return redirect(url_for('admin.list_api_key_types'))


@bp.route('/api-key-types/update/<int:key_type_id>', methods=['POST'])
@login_required
def update_api_key_type(key_type_id):
    """Update an existing API key type via stored procedure."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    code = request.form.get('code', '').strip()
    name = request.form.get('name', '').strip()
    description = request.form.get('description', '').strip()
    requires_company = request.form.get('requires_company') == 'true'
    allows_trial_limit = request.form.get('allows_trial_limit') == 'true'
    default_expiry_days = request.form.get('default_expiry_days', '').strip()
    order_num = request.form.get('order_num', '999')
    enabled = request.form.get('enabled') == 'true'
    notes = request.form.get('notes', '').strip()
    default_max_submissions = request.form.get('default_max_submissions', '').strip()
    rate_limit_period = request.form.get('rate_limit_period', '').strip() or None
    rate_limit_count = request.form.get('rate_limit_count', '').strip()

    if not code or not name:
        flash("Key type code and name are required.", "danger")
        return redirect(url_for('admin.list_api_key_types'))

    try:
        order_num = int(order_num)
    except ValueError:
        flash("Order must be a number.", "danger")
        return redirect(url_for('admin.list_api_key_types'))

    # Convert default_expiry_days to integer or None
    if default_expiry_days:
        try:
            default_expiry_days = int(default_expiry_days)
        except ValueError:
            flash("Default expiry days must be a number.", "danger")
            return redirect(url_for('admin.list_api_key_types'))
    else:
        default_expiry_days = None

    # Convert default_max_submissions to integer or None
    if default_max_submissions:
        try:
            default_max_submissions = int(default_max_submissions)
        except ValueError:
            flash("Default max submissions must be a number.", "danger")
            return redirect(url_for('admin.list_api_key_types'))
    else:
        default_max_submissions = None

    # Convert rate_limit_count to integer or None
    if rate_limit_count:
        try:
            rate_limit_count = int(rate_limit_count)
        except ValueError:
            flash("Rate limit count must be a number.", "danger")
            return redirect(url_for('admin.list_api_key_types'))
    else:
        rate_limit_count = None

    # Validate rate limit consistency
    if (rate_limit_period and not rate_limit_count) or (not rate_limit_period and rate_limit_count):
        flash("Rate limit period and count must both be set or both be empty.", "danger")
        return redirect(url_for('admin.list_api_key_types'))

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Use stored procedure (set_api_key_type is upsert - it will update existing)
        cursor.execute(
            "SELECT set_api_key_type(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (code, name, description, requires_company, allows_trial_limit,
             default_expiry_days, order_num, enabled, notes if notes else None,
             default_max_submissions, rate_limit_period, rate_limit_count)
        )
        conn.commit()
        flash(f"API key type '{name}' updated successfully.", "success")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        flash(f"Database error: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return redirect(url_for('admin.list_api_key_types'))


@bp.route('/api-key-types/delete/<int:key_type_id>', methods=['POST'])
@login_required
def delete_api_key_type(key_type_id):
    """Delete an API key type via stored procedure."""
    if not current_user.has_privilege('AdministerUsers'):
        abort(403)

    config = load_trends_config()
    db_config = config.get('database')
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # First get the code to delete by code (stored procedure uses code, not ID)
        cursor.execute(
            "SELECT key_type_code FROM fetch_api_key_types(FALSE) WHERE key_type_id = %s",
            (key_type_id,)
        )
        result = cursor.fetchone()

        if result:
            key_type_code = result[0]
            # Use stored procedure to delete
            cursor.execute("SELECT delete_api_key_type(%s)", (key_type_code,))
            conn.commit()
            flash(f"API key type '{key_type_code}' deleted successfully.", "success")
        else:
            flash("API key type not found.", "danger")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        # Extract the error message (may contain usage info)
        error_msg = str(e)
        flash(f"Database error: {error_msg}", "danger")
    finally:
        if conn:
            conn.close()

    return redirect(url_for('admin.list_api_key_types'))
