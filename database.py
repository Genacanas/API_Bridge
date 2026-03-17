import pyodbc
import os
from typing import Generator
from dotenv import load_dotenv

load_dotenv()

# Connection string components from environment
SERVER = os.environ.get('DB_SERVER', 'nichebreakerdb.database.windows.net')
DATABASE = os.environ.get('DB_NAME', 'dev-milco')
USERNAME = os.environ.get('DB_USER', 'backendTest')
PASSWORD = os.environ.get('DB_PASSWORD')

# Determine driver based on OS
# Railway/Render uses Linux with MS ODBC 18
DRIVER = os.environ.get('DB_DRIVER', '{ODBC Driver 18 for SQL Server}')
if os.name == 'nt':
    DRIVER = '{SQL Server}' # Fallback for local Windows, might need to be ODBC Driver 17

def get_db_connection() -> pyodbc.Connection:
    """Creates a connect to the Azure SQL Server using pyodbc for the MAIN database (dev-milco)."""
    conn_str = f"DRIVER={DRIVER};SERVER={SERVER};PORT=1433;DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=yes;TrustServerCertificate=yes;"
    if os.name == 'nt':
         conn_str = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=yes;TrustServerCertificate=yes;"
    return pyodbc.connect(conn_str)

def get_auth_db_connection() -> pyodbc.Connection:
    """Creates a connect to the Azure SQL Server using pyodbc for the AUTH database (backend)."""
    auth_db_name = os.environ.get('DB_AUTH_NAME', 'backend')
    auth_db_pwd = os.environ.get('DB_AUTH_PASSWORD')
    
    conn_str = f"DRIVER={DRIVER};SERVER={SERVER};PORT=1433;DATABASE={auth_db_name};UID={USERNAME};PWD={auth_db_pwd};Encrypt=yes;TrustServerCertificate=yes;"
    if os.name == 'nt':
         conn_str = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={auth_db_name};UID={USERNAME};PWD={auth_db_pwd};Encrypt=yes;TrustServerCertificate=yes;"
    return pyodbc.connect(conn_str)

def get_db() -> Generator[pyodbc.Connection, None, None]:
    """Dependency injection for FastAPI routes (main db)."""
    conn = get_db_connection()
    try:
        yield conn
    finally:
        conn.close()

def get_auth_db() -> Generator[pyodbc.Connection, None, None]:
    """Dependency injection for FastAPI routes (auth db)."""
    conn = get_auth_db_connection()
    try:
        yield conn
    finally:
        conn.close()
