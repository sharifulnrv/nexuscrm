import sqlite3
import pandas as pd
from datetime import datetime
import os

class DBHandler:
    def __init__(self, db_path='l.db'):
        self.db_path = db_path
        # Ensure database exists
        if not os.path.exists(db_path):
            from init_sqlite import init_db
            init_db(db_path)

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # This allows accessing columns by name
        return conn

    def get_users(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        rows = cursor.fetchall()
        users = []
        for row in rows:
            u = dict(row)
            # Map lowercase DB keys to legacy Uppercase keys for compatibility
            mapped = {
                'Username': u.get('username'),
                'Password': u.get('password'),
                'Role': u.get('role'),
                'Name': u.get('name'),
                'Phone': u.get('phone')
            }
            users.append(mapped)
        conn.close()
        return users

    def get_user_by_username(self, username):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE username=?", (username,))
        row = cursor.fetchone()
        conn.close()
        if not row: return None
        u = dict(row)
        return {
            'Username': u.get('username'),
            'Password': u.get('password'),
            'Role': u.get('role'),
            'Name': u.get('name'),
            'Phone': u.get('phone')
        }

    def update_user(self, username, data):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        new_username = data.get('Username') or data.get('username')
        
        fields = []
        params = []
        
        mapping = {
            'username': ['Username', 'username'],
            'password': ['Password', 'password'],
            'name': ['Name', 'name'],
            'phone': ['Phone', 'phone']
        }
        
        for db_col, keys in mapping.items():
            for k in keys:
                if k in data and data[k]: # Don't update empty strings for password if not provided
                    fields.append(f"{db_col}=?")
                    params.append(data[k])
                    break
        
        if not fields:
            conn.close()
            return False
            
        sql = f"UPDATE users SET {', '.join(fields)} WHERE username=?"
        params.append(username)
        
        cursor.execute(sql, params)
        
        # Cascading updates if username changed
        if new_username and new_username != username:
            cursor.execute("UPDATE leads SET agent=? WHERE agent=?", (new_username, username))
            cursor.execute("UPDATE notes_history SET agent=? WHERE agent=?", (new_username, username))
            cursor.execute("UPDATE handovers SET from_agent=? WHERE from_agent=?", (new_username, username))
            cursor.execute("UPDATE handovers SET to_agent=? WHERE to_agent=?", (new_username, username))

        conn.commit()
        conn.close()
        return True

    def authenticate_user(self, username, password):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE username=? AND password=?", (username, password))
        row = cursor.fetchone()
        conn.close()
        if row:
            u = dict(row)
            return {
                'Username': u.get('username'),
                'Password': u.get('password'),
                'Role': u.get('role'),
                'Name': u.get('name'),
                'Phone': u.get('phone')
            }
        return None

    def add_user(self, data):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Mapping for incoming user data
        mapping = {
            'username': ['Username', 'username'],
            'password': ['Password', 'password'],
            'role': ['Role', 'role'],
            'name': ['Name', 'name'],
            'phone': ['Phone', 'phone']
        }
        
        final_data = {}
        for db_col, keys in mapping.items():
            val = ''
            for k in keys:
                if k in data:
                    val = data[k]
                    break
            final_data[db_col] = val
            
        if not final_data.get('username') or not final_data.get('password'):
            conn.close()
            return False, "Username and password are required."
            
        try:
            columns = ', '.join(final_data.keys())
            placeholders = ', '.join(['?'] * len(final_data))
            sql = f"INSERT OR REPLACE INTO users ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(final_data.values()))
            conn.commit()
            conn.close()
            return True, "User saved successfully."
        except Exception as e:
            conn.close()
            return False, str(e)

    def _map_lead_row(self, row):
        if not row: return None
        d = dict(row)
        return {
            'ID': d.get('id'),
            'Name': d.get('name'),
            'Phone': d.get('phone'),
            'Profession': d.get('profession'),
            'Location': d.get('location'),
            'Address': d.get('address'),
            'Note': d.get('note'),
            'InterestStar': d.get('interest_star'),
            'VisitInterested': d.get('visit_interested'),
            'VisitDate': d.get('visit_date'),
            'RecordingDate': d.get('recording_date'),
            'FollowUpDate': d.get('followup_date'),
            'Status': d.get('status'),
            'Agent': d.get('agent'),
            'Timestamp': d.get('timestamp'),
            'LastUpdated': d.get('last_updated'),
            'LastCallDate': d.get('last_call_date')
        }

    def get_all_leads(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT l.*, 
                   l.interest_star as static_rating,
                   COALESCE((SELECT interest_star FROM notes_history WHERE lead_id = l.id AND interest_star != '' AND interest_star IS NOT NULL ORDER BY timestamp DESC LIMIT 1), l.interest_star) as interest_star,
                   (SELECT MAX(call_date) FROM notes_history WHERE lead_id = l.id AND call_date != '') as last_call_date
            FROM leads l
        """)
        rows = cursor.fetchall()
        leads = [self._map_lead_row(r) for r in rows]
        conn.close()
        return pd.DataFrame(leads) if leads else pd.DataFrame()

    def get_lead_by_id(self, lead_id):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM leads WHERE id=?", (lead_id,))
        row = cursor.fetchone()
        conn.close()
        return self._map_lead_row(row)

    def _get_lead_data_from_dict(self, input_dict):
        # Map of DB column (lowercase) to possible source keys (including legacy uppercase)
        mapping = {
            'id': ['id', 'ID'],
            'name': ['name', 'Name'],
            'phone': ['phone', 'Phone'],
            'profession': ['profession', 'Profession'],
            'location': ['location', 'Location'],
            'address': ['address', 'Address'],
            'note': ['note', 'Note', 'LastNote'],
            'interest_star': ['interest_star', 'InterestStar'],
            'visit_interested': ['visit_interested', 'VisitInterested'],
            'visit_date': ['visit_date', 'VisitDate'],
            'recording_date': ['recording_date', 'RecordingDate'],
            'followup_date': ['followup_date', 'FollowUpDate'],
            'status': ['status', 'Status'],
            'agent': ['agent', 'Agent'],
            'timestamp': ['timestamp', 'Timestamp'],
            'last_updated': ['last_updated', 'LastUpdated']
        }
        
        data = {}
        for db_col, keys in mapping.items():
            val = ''
            for k in keys:
                if k in input_dict:
                    val = input_dict[k]
                    break
            data[db_col] = val
        
        if not data['interest_star']: data['interest_star'] = 0
        return data

    def add_lead(self, lead_data):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        data = self._get_lead_data_from_dict(lead_data)
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        sql = f"INSERT OR REPLACE INTO leads ({columns}) VALUES ({placeholders})"
        
        cursor.execute(sql, list(data.values()))
        conn.commit()
        conn.close()
        return True

    def update_lead(self, lead_id, update_data):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Filter and map update_data
        full_data = self._get_lead_data_from_dict(update_data)
        # Remove keys that shouldn't be updated or are empty in update_data
        actual_updates = {}
        for k, v in full_data.items():
            # Check if either version of the key was in the original update_data
            if k in update_data or k.title() in update_data or k.replace('_', '') in [x.replace('_', '') for x in update_data.keys()]:
                actual_updates[k] = v
        
        if not actual_updates:
            conn.close()
            return True
            
        actual_updates['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        fields = []
        values = []
        for key, value in actual_updates.items():
            fields.append(f"{key}=?")
            values.append(value)
        
        values.append(lead_id)
        sql = f"UPDATE leads SET {', '.join(fields)} WHERE id=?"
        
        cursor.execute(sql, values)
        conn.commit()
        conn.close()
        return True

    def get_history(self, lead_id=None):
        conn = self._get_connection()
        if lead_id:
            df = pd.read_sql_query("SELECT * FROM notes_history WHERE lead_id=? ORDER BY timestamp DESC", conn, params=(lead_id,))
        else:
            df = pd.read_sql_query("SELECT * FROM notes_history ORDER BY timestamp DESC", conn)
        conn.close()
        
        history = []
        for _, row in df.iterrows():
            history.append(self._map_history_row(row))
        return history

    def get_history_by_id(self, history_id):
        conn = self._get_connection()
        query = """
            SELECT h.*, l.name as lead_name, l.phone as lead_phone
            FROM notes_history h
            LEFT JOIN leads l ON h.lead_id = l.id
            WHERE h.id = ?
        """
        df = pd.read_sql_query(query, conn, params=(history_id,))
        conn.close()
        
        if df.empty: return None
        row = df.iloc[0]
        d = self._map_history_row(row)
        d['CustomerName'] = row.get('lead_name')
        d['CustomerPhone'] = row.get('lead_phone')
        return d

    def get_global_history(self):
        conn = self._get_connection()
        query = """
            SELECT h.*, l.name as lead_name, l.phone as lead_phone
            FROM notes_history h
            LEFT JOIN leads l ON h.lead_id = l.id
            ORDER BY h.timestamp DESC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        history = []
        for _, row in df.iterrows():
            d = self._map_history_row(row)
            d['CustomerName'] = row.get('lead_name')
            d['CustomerPhone'] = row.get('lead_phone')
            history.append(d)
        return history

    def _map_history_row(self, row):
        if row is None: return None
        d = dict(row)
        return {
            'ID': d.get('id'),
            'LeadID': d.get('lead_id'),
            'Timestamp': d.get('timestamp'),
            'Agent': d.get('agent'),
            'Note': d.get('note'),
            'VisitInterested': d.get('visit_interested'),
            'VisitDate': d.get('visit_date'),
            'RecordingURL': d.get('call_recording'),
            'CallDate': d.get('call_date'),
            'FollowUpDate': d.get('followup_date'),
            'InterestStar': d.get('interest_star')
        }

    def add_history_entry(self, lead_id, entry_data):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Mapping for incoming history data
        mapping = {
            'lead_id': ['lead_id', 'LeadID'],
            'agent': ['agent', 'Agent'],
            'note': ['note', 'Note', 'LastNote'],
            'visit_interested': ['visit_interested', 'VisitInterested'],
            'visit_date': ['visit_date', 'VisitDate'],
            'call_recording': ['call_recording', 'CallRecording', 'RecordingURL'],
            'call_date': ['call_date', 'CallDate'],
            'followup_date': ['followup_date', 'FollowUpDate'],
            'interest_star': ['interest_star', 'InterestStar']
        }
        
        data = {}
        for db_col, keys in mapping.items():
            val = ''
            for k in keys:
                if k in entry_data:
                    val = entry_data[k]
                    break
            data[db_col] = val
            
        data['lead_id'] = lead_id
        data['timestamp'] = now
        
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        sql = f"INSERT INTO notes_history ({columns}) VALUES ({placeholders})"
        
        cursor.execute(sql, list(data.values()))
        
        # Synchronize rating with leads table if provided in history
        if data.get('interest_star') is not None and data.get('interest_star') != '':
            cursor.execute("UPDATE leads SET interest_star=?, last_updated=? WHERE id=?", 
                           (data['interest_star'], now, lead_id))
            
        conn.commit()
        conn.close()
        return True

    def initiate_handover(self, lead_id, lead_name, from_agent, to_agent):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM handovers WHERE lead_id=? AND status='Pending'", (lead_id,))
        if cursor.fetchone():
            conn.close()
            return False, "Handover already pending."
            
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''INSERT INTO handovers (lead_id, from_agent, to_agent, timestamp, status, lead_name) 
                          VALUES (?, ?, ?, ?, ?, ?)''', 
                       (lead_id, from_agent, to_agent, now, 'Pending', lead_name))
        
        self.add_history_entry(lead_id, {
            'Agent': from_agent,
            'Note': f"Handover initiated to {to_agent}."
        })
        
        conn.commit()
        conn.close()
        return True, "Handover initiated."

    def get_pending_handovers(self, username):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM handovers WHERE to_agent=? AND status='Pending'", (username,))
        rows = cursor.fetchall()
        conn.close()
        return [self._map_handover_row(r) for r in rows]

    def get_all_handovers(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM handovers")
        rows = cursor.fetchall()
        conn.close()
        return [self._map_handover_row(r) for r in rows]

    def _map_handover_row(self, row):
        if row is None: return None
        d = dict(row)
        return {
            'LeadID': d.get('lead_id'),
            'FromAgent': d.get('from_agent'),
            'ToAgent': d.get('to_agent'),
            'Timestamp': d.get('timestamp'),
            'Status': d.get('status'),
            'LeadName': d.get('lead_name')
        }

    def respond_to_handover(self, lead_id, action, current_agent):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM handovers WHERE lead_id=? AND status='Pending'", (lead_id,))
        handover = cursor.fetchone()
        
        if not handover:
            conn.close()
            return False, "Handover record not found."
            
        cursor.execute("UPDATE handovers SET status=? WHERE id=?", (action, handover['id']))
        
        self.add_history_entry(lead_id, {
            'Agent': current_agent,
            'Note': f"Handover {action.lower()}."
        })
        
        if action == 'Accepted':
            cursor.execute("UPDATE leads SET agent=?, last_updated=? WHERE id=?", 
                           (current_agent, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), lead_id))
            
        conn.commit()
        conn.close()
        return True, f"Handover {action.lower()} completed."

    def update_user_password(self, username, new_password):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET password=? WHERE username=?", (new_password, username))
        conn.commit()
        conn.close()
        return True
