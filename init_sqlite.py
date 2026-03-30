import sqlite3
import os

def init_db(db_path='l.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create Users table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password TEXT NOT NULL,
        role TEXT NOT NULL,
        name TEXT,
        phone TEXT
    )
    ''')

    # Create Leads table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY,
        name TEXT,
        phone TEXT,
        profession TEXT,
        location TEXT,
        address TEXT,
        note TEXT,
        interest_star INTEGER DEFAULT 0,
        visit_interested TEXT,
        visit_date TEXT,
        recording_date TEXT,
        followup_date TEXT,
        status TEXT,
        agent TEXT,
        timestamp TEXT,
        last_updated TEXT
    )
    ''')

    # Create NotesHistory table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS notes_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER,
        timestamp TEXT,
        agent TEXT,
        note TEXT,
        visit_interested TEXT,
        visit_date TEXT,
        call_recording TEXT,
        followup_date TEXT,
        interest_star INTEGER,
        FOREIGN KEY (lead_id) REFERENCES leads (id)
    )
    ''')

    # Create Handovers table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS handovers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER,
        from_agent TEXT,
        to_agent TEXT,
        timestamp TEXT,
        status TEXT,
        lead_name TEXT,
        FOREIGN KEY (lead_id) REFERENCES leads (id)
    )
    ''')

    # Seed initial users if they don't exist
    initial_users = [
        ('admin', 'admin', 'Admin', 'System Admin', '123456789'),
        ('mim', 'mim1234', 'Agent', 'Mim Akter', '987654321'),
        ('salam', 'salam1234', 'Agent', 'Salam', '17438346'),
        ('agent', 'agent1234', 'Agent', 'Test Agent', '000000000')
    ]

    for username, password, role, name, phone in initial_users:
        cursor.execute('INSERT OR IGNORE INTO users (username, password, role, name, phone) VALUES (?, ?, ?, ?, ?)',
                       (username, password, role, name, phone))

    conn.commit()
    conn.close()
    print(f"Database initialized at {db_path}")

if __name__ == "__main__":
    init_db()
