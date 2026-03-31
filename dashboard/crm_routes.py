from flask import Blueprint, render_template, request, redirect, url_for, flash, session, send_file
from utils.factory import get_db_handler
from utils.export_helper import export_to_csv, export_to_excel
from config import Config
from auth.login import login_required, admin_required
import os
import pandas as pd
from datetime import datetime
from werkzeug.utils import secure_filename

dashboard_bp = Blueprint('dashboard', __name__)
db = get_db_handler()

@dashboard_bp.route('/admin/history')
@admin_required
def admin_history():
    history = db.get_global_history()
    return render_template('admin_history.html', history=history)

@dashboard_bp.route('/history/<int:history_id>')
@login_required
def history_detail(history_id):
    entry = db.get_history_by_id(history_id)
    if not entry:
        flash("History record not found.", "danger")
        return redirect(url_for('dashboard.index'))
    return render_template('history_detail.html', entry=entry)

@dashboard_bp.route('/')
@login_required
def index():
    user = session['user']
    
    # Refresh user info from DB to reflect manual changes
    users = db.get_users()
    current_user_data = next((u for u in users if u['Username'] == user['Username']), None)
    if current_user_data:
        session['user']['Name'] = current_user_data.get('Name', user['Username'])
        session['user']['Role'] = current_user_data.get('Role', 'Agent')
        session.modified = True
        user = session['user']
    
    df_all = db.get_all_leads()
    
    # Calculate Global Stats (Static for Dashboard Header)
    if not df_all.empty:
        total_leads = len(df_all)
        hot_leads = len(df_all[df_all['InterestStar'].astype(str).isin(['4', '5'])])
        warm_leads = len(df_all[df_all['InterestStar'].astype(str) == '3'])
    else:
        total_leads = hot_leads = warm_leads = 0

    df = df_all.copy()
    
    # Search functionality
    q = request.args.get('q', '').strip()
    
    # Advanced Filters
    f_last_call = request.args.get('last_call', '').strip()
    f_followup = request.args.get('followup', '').strip()
    f_rating = request.args.get('rating', '').strip()
    f_agent = request.args.get('agent', '').strip()
    f_fresh = request.args.get('fresh', '').strip()

    if not df.empty:
        if q:
            df = df[
                df['Name'].str.contains(q, case=False, na=False) | 
                df['Phone'].astype(str).str.contains(q, case=False, na=False)
            ]
        
        if f_last_call:
            df = df[df['LastCallDate'] == f_last_call]
        if f_followup:
            df = df[df['FollowUpDate'] == f_followup]
        if f_rating:
            df = df[df['InterestStar'].astype(str) == f_rating]
        if f_agent:
            df = df[df['Agent'] == f_agent]
        if f_fresh == '1':
            df = df[(df['LastCallDate'].isna()) | (df['LastCallDate'] == '')]

    if user['Role'] == Config.ROLE_AGENT:
        # Agents see only their leads
        if not df.empty:
            df = df[df['Agent'] == user['Username']]
    
    # Stats for Agents (can be filtered)
    if not df.empty:
        agent_stats_summary = {
            'total': len(df),
            'hot': len(df[df['InterestStar'].astype(str).isin(['4', '5'])]),
            'warm': len(df[df['InterestStar'].astype(str) == '3']),
            'cold': len(df[df['InterestStar'].astype(str).isin(['1', '2'])])
        }
    else:
        agent_stats_summary = {'total': 0, 'hot': 0, 'warm': 0, 'cold': 0}
    
    # Today's Activity Report
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Interactions today
    history_all = db.get_history()
    
    # Filter for today
    history_today_all = [h for h in history_all if str(h['Timestamp']).startswith(today)]
    
    if user['Role'] == Config.ROLE_AGENT:
        history_today = [h for h in history_today_all if h['Agent'] == user['Username']]
        leads_added_today = len(df[df['Timestamp'].str.startswith(today) & (df['Agent'] == user['Username'])]) if not df.empty else 0
    else:
        history_today = history_today_all
        leads_added_today = len(df[df['Timestamp'].str.startswith(today)]) if not df.empty else 0

    daily_report = {
        'leads_added': leads_added_today,
        'calls_logged': len([h for h in history_today if h.get('Note')]),
        'visits_interested': len([h for h in history_today if h.get('VisitInterested') == 'Yes']),
        'followups_set': len([h for h in history_today if h.get('FollowUpDate')]),
        'rating_updates': len([h for h in history_today if h.get('InterestStar')]),
        'total_notes': len([h for h in history_today if h.get('Note')]),
        'recent_activities': history_today[:5]
    }
    
    # Agent performance breakdown (Admin only)
    agent_stats = []
    if user['Role'] == Config.ROLE_ADMIN:
        agents = db.get_users()
        for agent in agents:
            if agent['Role'] == Config.ROLE_AGENT:
                a_hist = [h for h in history_today_all if h['Agent'] == agent['Username']]
                a_leads = len(df[df['Timestamp'].str.startswith(today) & (df['Agent'] == agent['Username'])]) if not df.empty else 0
                agent_stats.append({
                    'name': agent['Name'] if agent.get('Name') else agent['Username'],
                    'leads': a_leads,
                    'calls': len(a_hist),
                    'visits': len([h for h in a_hist if h.get('VisitInterested') == 'Yes'])
                })

    # Today's tasks (Follow-ups for today)
    today_tasks = df[df['FollowUpDate'] == today].to_dict('records') if not df.empty else []
    
    # Overdue follow-ups
    overdue = df[(df['FollowUpDate'] < today) & (df['FollowUpDate'] != '')].to_dict('records') if not df.empty else []

    # Default sort by LastUpdated
    if not df.empty:
        # Convert LastUpdated to datetime for proper sorting if it's not already
        df['LastUpdated_dt'] = pd.to_datetime(df['LastUpdated'], errors='coerce')
        df = df.sort_values(by='LastUpdated_dt', ascending=False)
        leads = df.to_dict('records')
    else:
        leads = []

    # Pending handovers
    all_handovers = db.get_all_handovers()
    username = str(session['user']['Username']).lower().strip()
    
    # Initialize handover variables
    pending_handovers = []
    sent_handovers = []
    
    if user['Role'] == Config.ROLE_ADMIN:
        pending_handovers = [h for h in all_handovers if h['Status'] == 'Pending']
    else:
        for h in all_handovers:
            h_to = str(h.get('ToAgent', '')).lower().strip()
            h_from = str(h.get('FromAgent', '')).lower().strip()
            status = h.get('Status')
            
            if status == 'Pending':
                if h_to == username:
                    pending_handovers.append(h)
                if h_from == username:
                    sent_handovers.append(h)

    # Calculate percentages for UI progress bars
    hot_percent = (hot_leads / total_leads * 100) if total_leads > 0 else 0
    warm_percent = (warm_leads / total_leads * 100) if total_leads > 0 else 0
    overdue_percent = (len(overdue) / total_leads * 100) if total_leads > 0 else 0

    # Premium Analytics Data (ApexCharts)
    rating_dist = []
    for r in range(0, 6):
        count = len(df[df['InterestStar'].astype(str) == str(r)]) if not df.empty else 0
        rating_dist.append(count)
    
    agent_dist = []
    if user['Role'] == Config.ROLE_ADMIN:
        agents_list = [u for u in db.get_users() if u['Role'] == Config.ROLE_AGENT]
        for a in agents_list:
            a_count = len(df_all[df_all['Agent'] == a['Username']]) if not df_all.empty else 0
            agent_dist.append({'label': a['Name'] if a.get('Name') else a['Username'], 'value': a_count})
    else:
        # For agent, show call trend (last 7 days) if needed, but for now just their rating dist
        pass

    agents = [u for u in db.get_users() if u['Role'] == Config.ROLE_AGENT]
    
    if user['Role'] == Config.ROLE_ADMIN:
        return render_template('dashboard_admin.html', 
                             total_leads=total_leads,
                             hot_leads=hot_leads,
                             warm_leads=warm_leads,
                             overdue_percent=overdue_percent,
                             rating_dist=rating_dist,
                             agent_dist=agent_dist,
                             stats=agent_stats_summary, 
                             daily_report=daily_report, 
                             agent_stats=agent_stats, 
                             leads=leads, 
                             today_tasks=today_tasks, 
                             overdue=overdue, 
                             q=q, 
                             last_call=f_last_call,
                             followup=f_followup,
                             rating=f_rating,
                             agent_filter=f_agent,
                             fresh=f_fresh,
                             agents=agents,
                             pending_handovers=pending_handovers)
    else:
        return render_template('dashboard_agent.html', 
                             total_leads=total_leads,
                             hot_leads=hot_leads,
                             warm_leads=warm_leads,
                             hot_percent=hot_percent,
                             warm_percent=warm_percent,
                             overdue_percent=overdue_percent,
                             rating_dist=rating_dist,
                             stats=agent_stats_summary, 
                             daily_report=daily_report, 
                             leads=leads, 
                             today_tasks=today_tasks, 
                             overdue=overdue, 
                             q=q, 
                             last_call=f_last_call,
                             followup=f_followup,
                             rating=f_rating,
                             fresh=f_fresh,
                             pending_handovers=pending_handovers, 
                             sent_handovers=sent_handovers)

@dashboard_bp.route('/profile', methods=['GET', 'POST'])
@login_required
def edit_profile():
    user = session['user']
    if request.method == 'POST':
        name = request.form.get('name')
        phone = request.form.get('phone')
        new_username = request.form.get('username')
        old_password = request.form.get('old_password')
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')
        
        current_password = str(user['Password'])
        current_username = user['Username']
        
        if old_password != current_password:
            flash("Current password verification failed.", "danger")
        elif new_password and new_password != confirm_password:
            flash("New password and confirmation do not match.", "warning")
        else:
            # Check if username is changing and if it's available
            if new_username and new_username != current_username:
                existing_users = db.get_users()
                if any(u['Username'] == new_username for u in existing_users):
                    flash(f"Identity '{new_username}' is already in use.", "warning")
                    return render_template('profile.html', user=user)

            updated_data = {
                'Name': name,
                'Phone': phone,
                'Username': new_username if new_username else current_username,
            }
            if new_password:
                updated_data['Password'] = new_password
            
            if db.update_user(current_username, updated_data):
                # Refresh session user
                users = db.get_users()
                updated_user = next((u for u in users if u['Username'] == (new_username if new_username else current_username)), None)
                if updated_user:
                    session['user'] = updated_user
                    session.modified = True
                
                flash("Profile and security credentials updated successfully.", "success")
                return redirect(url_for('dashboard.index'))
            else:
                flash("Failed to update profile.", "danger")
            
    return render_template('profile.html', user=user)

@dashboard_bp.route('/handover/respond/<lead_id>/<action>')
@login_required
def respond_handover(lead_id, action):
    # Action can be 'Accepted' or 'Rejected'
    success, message = db.respond_to_handover(lead_id, action, session['user']['Username'])
    if success:
        flash(message, "success")
    else:
        flash(message, "danger")
    return redirect(url_for('dashboard.index'))

@dashboard_bp.route('/lead/add', methods=['GET', 'POST'])
@login_required
def add_lead():
    if request.method == 'POST':
        lead_data = request.form.to_dict()
        lead_data['Agent'] = session['user']['Username']
        # Generate a unique ID if not provided
        lead_data['ID'] = int(datetime.now().timestamp())
        lead_data['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if db.add_lead(lead_data):
            flash("Lead added successfully.", "success")
            return redirect(url_for('dashboard.index'))
        else:
            flash("Failed to add lead.", "danger")
            
    agents = [u for u in db.get_users() if u['Role'] == Config.ROLE_AGENT]
    return render_template('crm_form.html', action="Add", agents=agents)

@dashboard_bp.route('/lead/edit/<lead_id>', methods=['GET', 'POST'])
@login_required
def edit_lead(lead_id):
    lead_dict = db.get_lead_by_id(lead_id)
    
    if not lead_dict:
        flash("Lead not found.", "danger")
        return redirect(url_for('dashboard.index'))
    
    # Get history
    history = db.get_history(lead_id)

    if request.method == 'POST':
        new_agent = request.form.get('Agent')
        is_handover = new_agent != lead_dict['Agent'] and session['user']['Role'] != Config.ROLE_ADMIN
        
        updated_data = request.form.to_dict()
        is_call = 'log_call' in request.form
        
        if is_handover:
            success, message = db.initiate_handover(lead_id, lead_dict['Name'], session['user']['Username'], new_agent)
            if not success:
                flash(message, "warning")
            else:
                flash(message, "info")
            updated_data['Agent'] = lead_dict['Agent']
            
        # Log history only if meaningful interaction data is provided
        has_interaction = any([
            updated_data.get('LastNote'),
            updated_data.get('CallDate'),
            updated_data.get('RecordingURL'),
            updated_data.get('FollowUpDate')
        ])
        
        if has_interaction:
            db.add_history_entry(lead_id, {
                'Agent': session['user']['Username'],
                'Note': updated_data.get('LastNote') or 'Interaction logged',
                'VisitInterested': updated_data.get('VisitInterested'),
                'VisitDate': updated_data.get('VisitDate'),
                'FollowUpDate': updated_data.get('FollowUpDate'),
                'InterestStar': updated_data.get('InterestStar'),
                'CallDate': updated_data.get('CallDate'),
                'RecordingURL': updated_data.get('RecordingURL')
            })

        if db.update_lead(lead_id, updated_data):
            flash("Profile updated.", "success")
            return redirect(url_for('dashboard.index'))
        else:
            flash("Failed to update profile.", "danger")
            
    agents = [u for u in db.get_users() if u['Role'] == Config.ROLE_AGENT]
    return render_template('crm_form.html', action="Edit", lead=lead_dict, history=history, agents=agents)

@dashboard_bp.route('/lead/report/<lead_id>')
@login_required
def download_report(lead_id):
    lead_dict = db.get_lead_by_id(lead_id)
    
    if not lead_dict:
        flash("Lead not found.", "danger")
        return redirect(url_for('dashboard.index'))
    
    # Get history
    history = db.get_history(lead_id)
    
    # Create a report dataframe
    report_df = pd.DataFrame(history)
    
    # Add customer info to the top
    customer_info = pd.DataFrame([
        {'Timestamp': 'CUSTOMER INFO', 'Agent': '', 'Note': ''},
        {'Timestamp': f"Name: {lead_dict['Name']}", 'Agent': '', 'Note': ''},
        {'Timestamp': f"Phone: {lead_dict['Phone']}", 'Agent': '', 'Note': ''},
        {'Timestamp': f"Profession: {lead_dict['Profession']}", 'Agent': '', 'Note': ''},
        {'Timestamp': f"Address: {lead_dict['Address']}", 'Agent': '', 'Note': ''},
        {'Timestamp': '', 'Agent': '', 'Note': ''},
        {'Timestamp': 'INTERACTION HISTORY', 'Agent': '', 'Note': ''}
    ])
    
    final_df = pd.concat([customer_info, report_df], ignore_index=True)
    
    excel_path = os.path.join(Config.UPLOAD_FOLDER, f"report_{lead_dict['Phone']}.xlsx")
    export_to_excel(final_df, excel_path)
    
    return send_file(excel_path, as_attachment=True)

@dashboard_bp.route('/export')
@login_required
def export():
    df = db.get_all_leads()
    
    # Get filters from URL
    q = request.args.get('q', '').strip()
    f_last_call = request.args.get('last_call', '').strip()
    f_followup = request.args.get('followup', '').strip()
    f_rating = request.args.get('rating', '').strip()
    f_agent = request.args.get('agent', '').strip()
    f_fresh = request.args.get('fresh', '').strip()

    if not df.empty:
        if q:
            df = df[df['Name'].str.contains(q, case=False, na=False) | df['Phone'].astype(str).str.contains(q, case=False, na=False)]
        if f_last_call:
            df = df[df['LastCallDate'] == f_last_call]
        if f_followup:
            df = df[df['FollowUpDate'] == f_followup]
        if f_rating:
            df = df[df['InterestStar'].astype(str) == f_rating]
        if f_agent:
            df = df[df['Agent'] == f_agent]
        if f_fresh == '1':
            df = df[(df['LastCallDate'].isna()) | (df['LastCallDate'] == '')]

    if session['user']['Role'] == Config.ROLE_AGENT:
        df = df[df['Agent'] == session['user']['Username']]
    
    # Sort by LastUpdated for export as well
    if not df.empty:
        df['LastUpdated_dt'] = pd.to_datetime(df['LastUpdated'], errors='coerce')
        df = df.sort_values(by='LastUpdated_dt', ascending=False)
        if 'LastUpdated_dt' in df.columns:
            df = df.drop(columns=['LastUpdated_dt'])

    excel_path = os.path.join(Config.UPLOAD_FOLDER, 'leads_export.xlsx')
    export_to_excel(df, excel_path)
    return send_file(excel_path, as_attachment=True)

@dashboard_bp.route('/lead/delete/<lead_id>')
@login_required
def delete_lead(lead_id):
    # SQLite logic for deletion
    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM leads WHERE id=?", (lead_id,))
    cursor.execute("DELETE FROM notes_history WHERE lead_id=?", (lead_id,))
    conn.commit()
    conn.close()
    flash("Customer deleted successfully.", "success")
    return redirect(url_for('dashboard.index'))

@dashboard_bp.route('/admin/bulk-upload', methods=['GET', 'POST'])
@admin_required
def bulk_upload():
    if request.method == 'POST':
        raw_rows = []
        
        # Handle Copy-Paste
        paste_data = request.form.get('paste_data', '').strip()
        if paste_data:
            lines = paste_data.split('\n')
            for line in lines:
                parts = line.split(',')
                if len(parts) >= 1:
                    name = parts[0].strip() if len(parts) > 1 else ''
                    phone = parts[-1].strip()
                    raw_rows.append({'Name': name, 'Phone': phone})
        
        # Handle File Upload
        file = request.files.get('file')
        if file and file.filename.endswith('.csv'):
            import csv
            import io
            stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
            csv_reader = csv.DictReader(stream)
            for row in csv_reader:
                name = row.get('cname') or row.get('Name') or row.get('name') or ''
                phone = row.get('cphone') or row.get('Phone') or row.get('phone') or ''
                raw_rows.append({'Name': name, 'Phone': phone})
        elif file and (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
            file_df = pd.read_excel(file)
            for _, row in file_df.iterrows():
                name = row.get('cname') or row.get('Name') or row.get('name') or ''
                phone = row.get('cphone') or row.get('Phone') or row.get('phone') or ''
                raw_rows.append({'Name': str(name) if pd.notna(name) else '', 'Phone': str(phone) if pd.notna(phone) else ''})

        if not raw_rows:
            flash("No valid data found.", "warning")
            return redirect(url_for('dashboard.bulk_upload'))

        assigned_agent = request.form.get('assigned_agent', session['user']['Username'])
        
        # Get count of "Unknown name" in DB to continue numbering
        conn = db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM leads WHERE name LIKE 'Unknown name %'")
        unknown_counter = cursor.fetchone()[0]
        
        added = 0
        skipped = 0
        for item in raw_rows:
            name = item['Name']
            phone = str(item['Phone']).strip()
            if not phone: continue
            
            if phone.isdigit() and not phone.startswith('0'):
                phone = '0' + phone
            
            # Check for duplicate phone
            cursor.execute("SELECT id FROM leads WHERE phone=?", (phone,))
            if cursor.fetchone():
                skipped += 1
                continue
                
            if not name or name.lower() == 'nan':
                unknown_counter += 1
                name = f"Unknown name {unknown_counter}"
            
            db.add_lead({
                'ID': int(datetime.now().timestamp()) + added,
                'Name': name,
                'Phone': phone,
                'Agent': assigned_agent,
                'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            added += 1
            
        flash(f"Success! {added} leads added, {skipped} skipped.", "success")
        return redirect(url_for('dashboard.index'))

    agents = [u for u in db.get_users() if u['Role'] == Config.ROLE_AGENT]
    return render_template('bulk_upload.html', agents=agents)

@dashboard_bp.route('/admin/agents', methods=['GET', 'POST'])
@admin_required
def manage_agents():
    if request.method == 'POST':
        user_data = request.form.to_dict()
        conn = db._get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO users (username, password, role, name, phone) VALUES (?, ?, ?, ?, ?)",
                       (user_data.get('Username'), user_data.get('Password'), user_data.get('Role'), 
                        user_data.get('Name'), user_data.get('Phone')))
        conn.commit()
        conn.close()
        flash("Agent saved successfully.", "success")
            
    users = db.get_users()
    agents = [u for u in users if u['Role'] == Config.ROLE_AGENT]
    return render_template('manage_agents.html', agents=agents)

@dashboard_bp.route('/admin/agents/delete/<username>')
@admin_required
def delete_agent(username):
    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE username=?", (username,))
    conn.commit()
    conn.close()
    flash("Agent deleted.", "success")
    return redirect(url_for('dashboard.manage_agents'))

@dashboard_bp.route('/admin/agent/edit/<username>', methods=['GET', 'POST'])
@admin_required
def edit_agent(username):
    agent = db.get_user_by_username(username)
    if not agent:
        flash("Agent not found.", "danger")
        return redirect(url_for('dashboard.manage_agents'))
    
    if request.method == 'POST':
        updated_data = request.form.to_dict()
        # Only update if fields were changed
        if db.update_user(username, updated_data):
            flash(f"Agent {username} updated successfully.", "success")
            return redirect(url_for('dashboard.manage_agents'))
        else:
            flash("No changes made or failed to update.", "warning")
            
    return render_template('admin_agent_form.html', agent=agent)
