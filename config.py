import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'nexus-crm-secret-key-12345'
    
    # Database Configuration
    # Options: 'SHEETS' or 'SQLITE'
    DB_TYPE = 'SQLITE'
    DB_PATH = os.path.join(os.path.dirname(__file__), 'l.db')
    
    # Google Sheets Configuration (Legacy/Fallback)
    SPREADSHEET_ID = '1nD3DRbu7XPe2KJPDlrp58FBKm3DltyYwn2mt5RrGYrk'
    CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), 'credentials.json')
    
    # Folder Path
    UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
    
    # Lead Classification Ratings
    COLD_THRESHOLD = 2
    WARM_THRESHOLD = 3
    HOT_THRESHOLD = 5
    
    # Roles
    ROLE_ADMIN = 'Admin'
    ROLE_AGENT = 'Agent'

if not os.path.exists(Config.UPLOAD_FOLDER):
    os.makedirs(Config.UPLOAD_FOLDER)
