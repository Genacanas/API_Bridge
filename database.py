import pyodbc
import os
from typing import Generator
from dotenv import load_dotenv

load_dotenv()

# Connection string components from environment or fallback to defaults
SERVER = os.environ.get('DB_SERVER', 'nichebreakerdb.database.windows.net')
DATABASE = os.environ.get('DB_NAME', 'dev-milco')
USERNAME = os.environ.get('DB_USER', 'backendTest')
PASSWORD = os.environ.get('DB_PASSWORD', 'Xk9#mP2$vL7@nQ4!')

# Determine driver based on OS
# Railway/Render uses Linux with MS ODBC 18
DRIVER = os.environ.get('DB_DRIVER', '{ODBC Driver 18 for SQL Server}')
if os.name == 'nt':
    DRIVER = '{SQL Server}' # Fallback for local Windows, might need to be ODBC Driver 17

def get_db_connection() -> pyodbc.Connection:
    """Creates a connect to the Azure SQL Server using pyodbc."""
    
    # We use TrustServerCertificate=yes to avoid strict SSL cert issues in some environments
    conn_str = f"DRIVER={DRIVER};SERVER={SERVER};PORT=1433;DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=yes;TrustServerCertificate=yes;"
    
    if os.name == 'nt':
         conn_str = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=yes;TrustServerCertificate=yes;"
    
    conn = pyodbc.connect(conn_str)
    return conn

def get_db() -> Generator[pyodbc.Connection, None, None]:
    """Dependency injection for FastAPI routes."""
    conn = get_db_connection()
    try:
        yield conn
    finally:
        conn.close()
