import pymssql
from dotenv import load_dotenv
import os

load_dotenv('.env')

server = os.environ.get('DB_SERVER')
database = os.environ.get('DB_NAME')
username = os.environ.get('DB_USER')
password = os.environ.get('DB_PASSWORD')

try:
    print(f"Connecting to {server}...")
    conn = pymssql.connect(server=server, user=username, password=password, database=database)
    cursor = conn.cursor()
    
    print("Checking status counts in pagesProducts...")
    cursor.execute("SELECT status, COUNT(*) as count FROM pagesProducts GROUP BY status")
    for row in cursor.fetchall():
        print(f"Status: {row[0]}, Count: {row[1]}")
        
    print("\nChecking some rows...")
    cursor.execute("SELECT TOP 5 status, pageId FROM pagesProducts")
    for row in cursor.fetchall():
        print(row)
        
    conn.close()
except Exception as e:
    print(f"Error connecting or querying: {e}")
