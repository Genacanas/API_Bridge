import pymssql
import os
from dotenv import load_dotenv

load_dotenv()

server = os.environ.get('DB_SERVER')
database = os.environ.get('DB_NAME')
username = os.environ.get('DB_USER')
password = os.environ.get('DB_PASSWORD')

try:
    conn = pymssql.connect(server=server, user=username, password=password, database=database)
    cursor = conn.cursor()
    
    print("searchTerms schema:")
    cursor.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'searchTerms'")
    for row in cursor.fetchall():
        print(row)
        
    print("Niche ID sample:")
    cursor.execute("SELECT TOP 1 Id FROM niches")
    row = cursor.fetchone()
    print("Niche ID:", row[0] if row else "None")
    
    if not row:
        print("Inserting a dummy niche since none exists...")
        cursor.execute("INSERT INTO niches (name) OUTPUT INSERTED.Id VALUES ('Default Niche')")
        inserted = cursor.fetchone()
        if inserted:
            print("Created niche ID:", inserted[0])
            conn.commit()
            
    conn.close()
except Exception as e:
    print(f"Error: {e}")
