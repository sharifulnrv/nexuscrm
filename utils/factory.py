from config import Config
from utils.db_handler import DBHandler

def get_db_handler():
    # Only SQLite is supported now as Google Sheets integration has been removed
    return DBHandler(Config.DB_PATH)
