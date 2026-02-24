import os
import urllib.parse
from sqlalchemy import create_engine, MetaData, Table

# Connection string components from C# code
SERVER = 'nichebreakerdb.database.windows.net'
DATABASE = 'dev-milco'
USERNAME = 'backendTest'
PASSWORD = 'Xk9#mP2$vL7@nQ4!'

# Format the connection string for SQLAlchemy using mssql+pymssql
# pymssql is much easier to install on Windows/Mac without ODBC drivers
connection_string = f"mssql+pymssql://{USERNAME}:{urllib.parse.quote_plus(PASSWORD)}@{SERVER}/{DATABASE}"

engine = create_engine(connection_string)
metadata = MetaData()

# Tables we care about
tables_to_inspect = ['pagesProducts', 'pages', 'ads', 'adCreatives']

try:
    with engine.connect() as conn:
        print("Successfully connected to the database!")
        print("-" * 50)
        metadata.reflect(bind=engine)
        
        for table_name in tables_to_inspect:
            if table_name in metadata.tables:
                print(f"Table: {table_name}")
                table = metadata.tables[table_name]
                for column in table.columns:
                    print(f"  - {column.name}: {column.type}")
                print("-" * 50)
            else:
                # SQL Server tables might have different cases or prefixes like dbo.pages
                found = False
                for t in metadata.tables:
                    if t.lower().endswith(table_name.lower()):
                        print(f"Found related table: {t}")
                        table = metadata.tables[t]
                        for column in table.columns:
                            print(f"  - {column.name}: {column.type}")
                        print("-" * 50)
                        found = True
                
                if not found:
                    print(f"Table '{table_name}' not found in reflected metadata.")
                    print("-" * 50)
except Exception as e:
    print(f"Error connecting or reflecting: {e}")
