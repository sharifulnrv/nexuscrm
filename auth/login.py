from flask import Blueprint, render_template, redirect, url_for, request, flash, session
from functools import wraps
from utils.factory import get_db_handler
from config import Config

auth_bp = Blueprint('auth', __name__)
db = get_db_handler()

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session or session['user']['Role'] != Config.ROLE_ADMIN:
            flash("Access denied. Admin privileges required.", "danger")
            return redirect(url_for('dashboard.index'))
        return f(*args, **kwargs)
    return decorated_function

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        users = db.get_users()
        user = next((u for u in users if u['Username'] == username and str(u['Password']) == password), None)
        
        if user:
            session['user'] = user
            flash(f"Welcome back, {user['Name']}!", "success")
            return redirect(url_for('dashboard.index'))
        else:
            flash("Invalid username or password.", "danger")
            
    return render_template('login.html')

@auth_bp.route('/logout')
def logout():
    session.pop('user', None)
    flash("You have been logged out.", "info")
    return redirect(url_for('auth.login'))
