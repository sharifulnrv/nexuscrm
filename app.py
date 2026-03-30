from flask import Flask, redirect, url_for, session, send_from_directory
from config import Config
from auth.login import auth_bp
from dashboard.crm_routes import dashboard_bp
import os

app = Flask(__name__)
app.config.from_object(Config)

# Register py
app.register_blueprint(auth_bp, url_prefix='/auth')
app.register_blueprint(dashboard_bp, url_prefix='/dashboard')

@app.route('/')
def index():
    if 'user' in session:
        return redirect(url_for('dashboard.index'))
    return redirect(url_for('auth.login'))

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
    # Run on 0.0.0.0 for LAN access as requested
    app.run(host='0.0.0.0', port=5000, debug=True)
