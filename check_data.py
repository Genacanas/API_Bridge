import pyodbc
from dotenv import load_dotenv
import os

load_dotenv('.env')

connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{os.environ.get('DB_SERVER')},1433;Database={os.environ.get('DB_NAME')};Uid={os.environ.get('DB_USER')};Pwd={os.environ.get('DB_PASSWORD')};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    print("Checking status counts in pagesProducts...")
    cursor.execute("SELECT status, COUNT(*) as count FROM pagesProducts GROUP BY status")
    for row in cursor.fetchall():
        print(f"Status: {row.status}, Count: {row.count}")
        
    print("\nChecking a few sample rows from pagesProducts...")
    cursor.execute("SELECT TOP 5 pp.pageId, pp.status, pg.Name, pg.eu_total_reach FROM pagesProducts pp INNER JOIN pages pg ON pp.pageId = pg.Id")
    for row in cursor.fetchall():
        print(row)
        
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
