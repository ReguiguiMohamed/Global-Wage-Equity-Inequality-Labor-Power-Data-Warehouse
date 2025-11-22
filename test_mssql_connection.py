import pyodbc
import pandas as pd
from pathlib import Path

def test_mssql_connection(server, database, username, password, port=1443):
    """
    Test script to verify MSSQL connection credentials
    """
    print(f"Testing connection to: {server}:{port}")
    print(f"Database: {database}")
    print(f"Username: {username}")
    
    # Different driver options to try
    drivers_to_try = [
        "{ODBC Driver 17 for SQL Server}",
        "{ODBC Driver 18 for SQL Server}",
        "{ODBC Driver 13 for SQL Server}",
        "{SQL Server}"
    ]
    
    connection_string = None
    cnxn = None
    
    for driver in drivers_to_try:
        try:
            print(f"\nTrying driver: {driver}")
            connection_string = (
                f"DRIVER={driver};"
                f"SERVER={server},{port};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout=30;"
                f"Login Timeout=30;"
            )
            
            cnxn = pyodbc.connect(connection_string)
            print(f"✓ SUCCESS: Connected using {driver}")
            break
        except Exception as e:
            print(f"✗ FAILED: {str(e)}")
            continue
    
    if cnxn is None:
        print("\n❌ All drivers failed. Connection could not be established.")
        return False
    
    try:
        # Test a simple query
        cursor = cnxn.cursor()
        cursor.execute("SELECT @@VERSION AS Version;")
        row = cursor.fetchone()
        print(f"✅ Database Version: {row[0] if row else 'Unknown'}")
        
        # Test querying a simple table to ensure full access
        cursor.execute("SELECT 1 as test_connection;")
        row = cursor.fetchone()
        print(f"✅ Query Test Result: {row[0] if row else 'No result'}")
        
        # List databases to confirm access
        cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')")
        databases = cursor.fetchall()
        print(f"✅ Available databases: {[db[0] for db in databases]}")
        
        print("\n🎉 Connection test PASSED! Your credentials are correct.")
        print(f"Use these settings in Airflow UI:")
        print(f"  Host: {server}")
        print(f"  Port: {port}")
        print(f"  Login: {username}")
        print(f"  Password: {'*' * len(password)} (hidden)")
        print(f"  Schema/Database: {database}")
        print(f"  Connection Type: Microsoft SQL Server")
        print(f"  Connection ID: mssql_default (or your preferred name)")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection established but test queries failed: {str(e)}")
        return False
    finally:
        if cnxn:
            cnxn.close()
            print("\n🔒 Connection closed.")

if __name__ == "__main__":
    print("MSSQL Connection Test Script")
    print("=" * 40)
    
    # Hardcoded test values - UPDATE THESE WITH YOUR ACTUAL CREDENTIALS
    server = "localhost"  # Your server IP
    database = "DW_Inequality"  # Your database name
    username = ""  # Your SQL username
    password = ""  # Your SQL password
    port = 1433  # Your port number
    
    print(f"Testing with:")
    print(f"  Server: {server}")
    print(f"  Database: {database}")
    print(f"  Username: {username}")
    print(f"  Port: {port}")
    print("")
    
    success = test_mssql_connection(server, database, username, password, port)
    
    if not success:
        print("\n🔧 Troubleshooting tips:")
        print("   1. Verify MSSQL is configured to accept remote connections")
        print("   2. Check Windows Firewall allows port 1443")
        print("   3. Confirm your user account has access to the database")
        print("   4. Try connecting with SSMS first to verify credentials")
        print("   5. Check if TCP/IP is enabled in MSSQL Configuration Manager")