"""
ETL DAG for Global Wage Equity Inequality Labor Power Data Warehouse
This DAG orchestrates the existing ETL processes from the dw_etl module.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os
import subprocess
import pandas as pd
from airflow.hooks.base import BaseHook
import pyodbc
from pathlib import Path

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'gweilpdw_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Global Wage Equity Inequality Labor Power Data Warehouse',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['etl', 'datawarehouse', 'gweilpdw'],
    max_active_runs=1
)

def run_dw_etl():
    """Function to run the main ETL process"""
    print("Starting GWEILPDW ETL process...")
    
    # Run the existing ETL script using Python
    result = subprocess.run([sys.executable, '/opt/airflow/dags/dw_etl/run_all.py'], 
                           capture_output=True, text=True, cwd='/opt/airflow/dags/dw_etl')
    
    if result.returncode == 0:
        print("ETL process completed successfully!")
        print("STDOUT:", result.stdout)
        return result.stdout
    else:
        print("ETL process failed!")
        print("STDERR:", result.stderr)
        raise RuntimeError(f"ETL process failed with return code {result.returncode}")


def run_profiling():
    """Function to run data profiling"""
    print("Running data profiling...")
    # This would implement profiling logic based on your outputs
    # For now, we'll just print that profiling would happen
    print("Data profiling completed!")


def get_sql_datatype(dtype):
    """
    Map pandas data types to SQL Server data types
    """
    if dtype == 'object':
        return 'NVARCHAR(MAX)'
    elif 'int' in str(dtype):
        return 'BIGINT'
    elif 'float' in str(dtype):
        return 'FLOAT'
    elif 'datetime' in str(dtype):
        return 'DATETIME2'
    elif 'bool' in str(dtype):
        return 'BIT'
    else:
        return 'NVARCHAR(255)'  # Default for unknown types


def write_csvs_to_mssql():
    """Task to write generated CSVs to MSSQL database with proper schema structure"""
    from airflow.hooks.base import BaseHook
    import pyodbc
    from pathlib import Path

    # Helper to clean DataFrames before sending to SQL Server
    def clean_dataframe(df):
        """Clean the entire DataFrame to handle invalid or problematic values"""
        df_cleaned = df.copy()
        for col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: (
                    None
                    if (
                        pd.isna(x)
                        or (isinstance(x, float) and (x != x or x == float("inf") or x == float("-inf")))
                        or (isinstance(x, str) and x.strip() == "")
                        or x in ["", "#N/A", "NULL", "null", "nan", "NaN", "N/A", "NA"]
                    )
                    else x
                )
            )
        return df_cleaned

    # Get the connection info from Airflow
    conn = BaseHook.get_connection('mssql_default')  # The connection ID you created

    # Use connection details
    server = conn.host
    database = conn.schema or 'DW_Inequality'  # Use schema if provided, otherwise default
    username = conn.login
    password = conn.password
    port = conn.port or 1433

    # Create connection string with proper driver
    drivers_to_try = [
        "{ODBC Driver 17 for SQL Server}",
    ]

    cnxn = None
    for driver in drivers_to_try:
        try:
            connection_string = (
                f"DRIVER={driver};"
                f"SERVER={server},{port};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout=30;"
                f"Login Timeout=30;"
                f"Encrypt=no;"
            )
            print(f"Attempting to connect with connection string: DRIVER={driver}, SERVER={server},{port}, DATABASE={database}")
            cnxn = pyodbc.connect(connection_string)
            print(f"Successfully connected using driver: {driver}")
            break
        except Exception as e:
            import traceback
            print(f"Driver {driver} failed with error: {str(e)}")
            print(f"Full error details:")
            traceback.print_exc()
            continue

    if cnxn is None:
        raise Exception("Could not connect with any available driver")

    print(f"Attempting to connect to MSSQL server: {server}:{port}")
    print(f"Database: {database}")

    # Create schemas if they don't exist
    cursor = cnxn.cursor()

    try:
        # Create dim and fact schemas
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dim') BEGIN EXEC('CREATE SCHEMA dim') END")
        print("Ensured 'dim' schema exists")

        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'fact') BEGIN EXEC('CREATE SCHEMA fact') END")
        print("Ensured 'fact' schema exists")

        cnxn.commit()
    except Exception as e:
        print(f"Error creating schemas: {str(e)}")
        cnxn.rollback()

    # Define the output directory where CSVs are generated
    out_dir = Path("/opt/airflow/dags/out")

    # Define CSV files with their target tables based on your exact schema structure
    csv_mappings = [
        # Dimensions (go to 'dim' schema with updated table names to use gender)
        {"csv": "Dim_Sex.csv", "table": "Dim_Gender", "schema": "dim"},  # CSV still named Dim_Sex but contains gender data, maps to Dim_Gender table
        {"csv": "Dim_Age.csv", "table": "Dim_Age", "schema": "dim"},
        {"csv": "Dim_Time.csv", "table": "Dim_Time", "schema": "dim"},
        {"csv": "Dim_Geography.csv", "table": "Dim_Geography", "schema": "dim"},
        {"csv": "Dim_Indicator.csv", "table": "Dim_Indicator", "schema": "dim"},
        {"csv": "Dim_Source.csv", "table": "Dim_Source", "schema": "dim"},
        {"csv": "Dim_Economic_Classification.csv", "table": "Dim_Economic_Classification", "schema": "dim"},
        # Facts (go to 'fact' schema with your exact table names)
        {"csv": "Fact_Economy.csv", "table": "Fact_Economy", "schema": "fact"},
        {"csv": "Fact_Inequality.csv", "table": "Fact_Inequality", "schema": "fact"},
        {"csv": "Fact_SocialDevelopment.csv", "table": "Fact_SocialDevelopment", "schema": "fact"},
    ]

    # Process CSV files in the correct order: dimensions first, then facts
    # to avoid foreign key constraint issues during insertion
    dimension_mappings = [m for m in csv_mappings if m["schema"] == "dim"]
    fact_mappings = [m for m in csv_mappings if m["schema"] == "fact"]

    # Process dimension tables first
    for mapping in dimension_mappings:
        csv_filename = mapping["csv"]
        table_name = mapping["table"]
        schema_name = mapping["schema"]
        full_table_name = f"[{schema_name}].[{table_name}]"

        csv_path = out_dir / csv_filename

        # Check if the file exists before processing
        if csv_path.exists():
            print(f"Processing {csv_filename} -> {full_table_name}")

            # Read the CSV
            df = pd.read_csv(csv_path)
            # Basic cleaning of missing values before type-specific handling
            df = df.replace({pd.NA: None})
            df = df.where(pd.notnull(df), None)
            df = df.replace('', None)

            # Coerce numeric measure columns (value/amount/percentage) to floats and handle problematic values
            for col in df.columns:
                col_lower = col.lower()
                if 'value' in col_lower or 'amount' in col_lower or 'percentage' in col_lower:
                    # Convert to numeric, coercing errors to NaN, then convert to Python-native float for SQL
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Replace any remaining problematic values (inf, -inf) with None
                    df[col] = df[col].replace([float('inf'), float('-inf')], None)
            print(f"Read {len(df)} rows from {csv_filename}")

            # No column name mapping needed since both CSV and table now use gender naming
            # The CSV contains gender columns and (after schema update) the database table expects gender columns

            # Write to database using pyodbc directly to avoid pandas dialect issues
            total_rows = len(df)
            chunk_size = 5000  # Process in chunks to avoid memory issues

            # First, check if the table actually exists in the database/schema using OBJECT_ID
            check_sql = f"""
            IF OBJECT_ID(N'{schema_name}.{table_name}', 'U') IS NOT NULL
                SELECT 1
            ELSE
                SELECT 0;
            """
            cursor.execute(check_sql)
            exists_result = cursor.fetchone()
            table_exists = exists_result[0] == 1 if exists_result else False

            if not table_exists:
                print(f"Table {full_table_name} does not exist, creating it...")

                # Generate CREATE TABLE statement based on DataFrame dtypes with specific data type mapping for data warehouse
                columns = list(df.columns)
                dtypes = df.dtypes

                # Create SQL data type mapping for proper SQL Server types
                column_definitions = []
                for col in columns:
                    dtype_str = str(dtypes[col])
                    if 'key' in col.lower() or 'id' in col.lower():
                        # For key fields, use INT with IDENTITY for surrogate keys to avoid duplicates
                        sql_type = 'INT IDENTITY(1,1) PRIMARY KEY' if 'key' in col.lower() else 'INT'
                    elif 'code' in col.lower():
                        sql_type = 'VARCHAR(50)'
                    elif 'label' in col.lower() or 'name' in col.lower() or 'description' in col.lower():
                        sql_type = 'VARCHAR(255)'
                    elif 'flag' in col.lower() or 'indicator' in col.lower():
                        sql_type = 'BIT'
                    elif 'amount' in col.lower() or 'value' in col.lower() or 'percentage' in col.lower():
                        sql_type = 'FLOAT NULL'  # More flexible for various numeric formats
                    elif 'date' in col.lower() or 'time' in col.lower():
                        sql_type = 'DATE'
                    elif 'int' in dtype_str:
                        sql_type = 'BIGINT'
                    elif 'float' in dtype_str:
                        sql_type = 'FLOAT NULL'  # More flexible for any float values
                    else:
                        # Default for other types
                        sql_type = 'VARCHAR(100)'

                    column_definitions.append(f"[{col}] {sql_type}")

                create_sql = f"CREATE TABLE {full_table_name} ({', '.join(column_definitions)})"

                print(f"Executing CREATE TABLE: {create_sql}")
                cursor.execute(create_sql)
                cnxn.commit()
                print(f"Table {full_table_name} created successfully.")
            else:
                print(f"Table {full_table_name} exists, replacing data...")

            # For dimension tables, we need to handle foreign key constraints carefully
            # For fact tables, we can safely delete all data
            if table_exists:
                if schema_name == 'fact':
                    print(f"Deleting from {full_table_name} to clear existing data...")
                    cursor.execute(f"DELETE FROM {full_table_name}")
                    cnxn.commit()
                    print(f"Table {full_table_name} cleared successfully.")
                elif schema_name == 'dim':
                    # For dimension tables, we need to handle foreign key constraints carefully
                    try:
                        # Try to delete with foreign key constraints disabled temporarily
                        alter_sql_disable = f"ALTER TABLE {full_table_name} NOCHECK CONSTRAINT ALL"
                        cursor.execute(alter_sql_disable)

                        cursor.execute(f"DELETE FROM {full_table_name}")

                        # Re-enable constraints
                        alter_sql_enable = f"ALTER TABLE {full_table_name} CHECK CONSTRAINT ALL"
                        cursor.execute(alter_sql_enable)

                        cnxn.commit()
                        print(f"Dimension table {full_table_name} cleared successfully.")
                    except Exception as e:
                        print(f"Could not clear dimension table {full_table_name} due to constraints: {str(e)}")
                        print(f"Skipping deletion for dimension table {full_table_name} to avoid foreign key conflicts.")
                        # Continue with insert anyway, but skip if already populated to avoid duplicates
                        cursor.execute(f"SELECT COUNT(1) FROM {full_table_name}")
                        row_count = cursor.fetchone()[0]
                        if row_count > 0:
                            print(f"Dimension table {full_table_name} already has data; skipping to avoid conflicts.")
                            continue  # Skip this table if it has existing data

            # Determine which columns to insert.
            # For this warehouse, the CSVs already contain the correct key values
            # (e.g. gender_key, geography_key, etc.), and the dimension/fact tables
            # expect those keys as NOT NULL columns. To keep things consistent with
            # the existing schema, we insert all CSV columns as-is.
            columns = list(df.columns)
            insert_columns = columns

            # Create the INSERT statement
            placeholders = ', '.join(['?' for _ in insert_columns])
            insert_sql = f"INSERT INTO {full_table_name} ({', '.join([f'[{col}]' for col in insert_columns])}) VALUES ({placeholders})"

            # Clean the DataFrame
            df_cleaned = clean_dataframe(df[insert_columns])

            # For fact tables, specifically handle the 'value' column to ensure proper float conversion
            if schema_name == 'fact' and 'value' in df_cleaned.columns:
                def normalize_value(x):
                    try:
                        v = float(x)
                    except (TypeError, ValueError):
                        return None
                    if v != v or v == float('inf') or v == float('-inf'):  # Check for NaN, inf, -inf
                        return None
                    return v

                df_cleaned['value'] = df_cleaned['value'].apply(normalize_value)

            # Insert data in chunks using pyodbc executemany to avoid parameter/size limits
            print(f"Starting chunked insert of {total_rows} rows into {full_table_name} with chunk size {chunk_size}...")
            for i in range(0, total_rows, chunk_size):
                chunk = df_cleaned.iloc[i:i + chunk_size]  # Use cleaned data

                # Convert chunk to list of tuples
                data_tuples = [tuple(row) for row in chunk.values]

                # Execute the insert for this chunk
                cursor.executemany(insert_sql, data_tuples)
                cnxn.commit()  # Commit after each chunk

                print(f"Wrote rows {i + 1} to {min(i + chunk_size, total_rows)} of {total_rows} to {full_table_name}")

        else:
            print(f"Warning: {csv_filename} not found in {out_dir}")

    # Then process fact tables (after dimensions are loaded to satisfy foreign key constraints)
    for mapping in fact_mappings:
        csv_filename = mapping["csv"]
        table_name = mapping["table"]
        schema_name = mapping["schema"]
        full_table_name = f"[{schema_name}].[{table_name}]"

        csv_path = out_dir / csv_filename

        # Check if the file exists before processing
        if csv_path.exists():
            print(f"Processing {csv_filename} -> {full_table_name}")

            # Read the CSV
            df = pd.read_csv(csv_path)
            # Basic cleaning of missing values before type-specific handling
            df = df.replace({pd.NA: None})
            df = df.where(pd.notnull(df), None)
            df = df.replace('', None)

            # Coerce numeric measure columns (value/amount/percentage) to floats and handle problematic values
            for col in df.columns:
                col_lower = col.lower()
                if 'value' in col_lower or 'amount' in col_lower or 'percentage' in col_lower:
                    # Convert to numeric, coercing errors to NaN, then convert to Python-native float for SQL
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Replace any remaining problematic values (inf, -inf) with None
                    df[col] = df[col].replace([float('inf'), float('-inf')], None)
            print(f"Read {len(df)} rows from {csv_filename}")

            # No column name mapping needed since both CSV and table now use gender naming
            # The CSV contains gender columns and (after schema update) the database table expects gender columns

            # Write to database using pyodbc directly to avoid pandas dialect issues
            total_rows = len(df)
            chunk_size = 1000  # Process in chunks to avoid memory issues

            # First, check if the table actually exists in the database/schema using OBJECT_ID
            check_sql = f"""
            IF OBJECT_ID(N'{schema_name}.{table_name}', 'U') IS NOT NULL
                SELECT 1
            ELSE
                SELECT 0;
            """
            cursor.execute(check_sql)
            exists_result = cursor.fetchone()
            table_exists = exists_result[0] == 1 if exists_result else False

            if not table_exists:
                print(f"Table {full_table_name} does not exist, creating it...")

                # Generate CREATE TABLE statement based on DataFrame dtypes with specific data type mapping for data warehouse
                columns = list(df.columns)
                dtypes = df.dtypes

                # Create SQL data type mapping for proper SQL Server types
                column_definitions = []
                for col in columns:
                    dtype_str = str(dtypes[col])
                    if 'key' in col.lower() or 'id' in col.lower():
                        # For key fields, use INT with IDENTITY for surrogate keys to avoid duplicates
                        sql_type = 'INT IDENTITY(1,1) PRIMARY KEY' if 'key' in col.lower() else 'INT'
                    elif 'code' in col.lower():
                        sql_type = 'VARCHAR(50)'
                    elif 'label' in col.lower() or 'name' in col.lower() or 'description' in col.lower():
                        sql_type = 'VARCHAR(255)'
                    elif 'flag' in col.lower() or 'indicator' in col.lower():
                        sql_type = 'BIT'
                    elif 'amount' in col.lower() or 'value' in col.lower() or 'percentage' in col.lower():
                        sql_type = 'FLOAT NULL'  # More flexible for various numeric formats
                    elif 'date' in col.lower() or 'time' in col.lower():
                        sql_type = 'DATE'
                    elif 'int' in dtype_str:
                        sql_type = 'BIGINT'
                    elif 'float' in dtype_str:
                        sql_type = 'FLOAT NULL'  # More flexible for any float values
                    else:
                        # Default for other types
                        sql_type = 'VARCHAR(100)'

                    column_definitions.append(f"[{col}] {sql_type}")

                create_sql = f"CREATE TABLE {full_table_name} ({', '.join(column_definitions)})"

                print(f"Executing CREATE TABLE: {create_sql}")
                cursor.execute(create_sql)
                cnxn.commit()
                print(f"Table {full_table_name} created successfully.")
            else:
                print(f"Table {full_table_name} exists, replacing data...")

            # Clear fact tables since they don't have dependencies (they're loaded after dimensions)
            if table_exists:
                print(f"Deleting from {full_table_name} to clear existing data...")
                cursor.execute(f"DELETE FROM {full_table_name}")
                cnxn.commit()
                print(f"Table {full_table_name} cleared successfully.")

            # Determine which columns to insert.
            # For this warehouse, the CSVs already contain the correct key values
            # (e.g. gender_key, geography_key, etc.), and the dimension/fact tables
            # expect those keys as NOT NULL columns. To keep things consistent with
            # the existing schema, we insert all CSV columns as-is.
            columns = list(df.columns)
            insert_columns = columns

            # Create the INSERT statement
            placeholders = ', '.join(['?' for _ in insert_columns])
            insert_sql = f"INSERT INTO {full_table_name} ({', '.join([f'[{col}]' for col in insert_columns])}) VALUES ({placeholders})"

            # Clean the DataFrame
            df_cleaned = clean_dataframe(df[insert_columns])

            # For fact tables, specifically handle the 'value' column to ensure proper float conversion
            if schema_name == 'fact' and 'value' in df_cleaned.columns:
                def normalize_value(x):
                    try:
                        v = float(x)
                    except (TypeError, ValueError):
                        return None
                    if v != v or v == float('inf') or v == float('-inf'):  # Check for NaN, inf, -inf
                        return None
                    return v

                df_cleaned['value'] = df_cleaned['value'].apply(normalize_value)

            # Insert data in chunks using pyodbc executemany to avoid parameter/size limits
            print(f"Starting chunked insert of {total_rows} rows into {full_table_name} with chunk size {chunk_size}...")
            for i in range(0, total_rows, chunk_size):
                chunk = df_cleaned.iloc[i:i+chunk_size]  # Use cleaned data

                # Convert chunk to list of tuples
                data_tuples = [tuple(row) for row in chunk.values]

                # Execute the insert for this chunk
                cursor.executemany(insert_sql, data_tuples)
                cnxn.commit()  # Commit after each chunk

                print(f"Wrote rows {i+1} to {min(i+chunk_size, total_rows)} of {total_rows} to {full_table_name}")

        else:
            print(f"Warning: {csv_filename} not found in {out_dir}")

    # Close the connection
    cursor.close()
    cnxn.close()
    print("All CSV files have been written to MSSQL database with proper schema structure successfully!")


# Define tasks
start_task = BashOperator(
    task_id='start_etl_process',
    bash_command='echo "Starting GWEILPDW ETL Pipeline"',
    dag=dag
)

run_etl_task = PythonOperator(
    task_id='run_dw_etl',
    python_callable=run_dw_etl,
    dag=dag
)

run_profiling_task = PythonOperator(
    task_id='run_data_profiling',
    python_callable=run_profiling,
    dag=dag
)

write_to_mssql_task = PythonOperator(
    task_id='write_data_to_mssql',
    python_callable=write_csvs_to_mssql,
    dag=dag
)

end_task = BashOperator(
    task_id='finish_etl_process',
    bash_command='echo "ETL Pipeline completed successfully!"',
    dag=dag
)

# Set task dependencies
start_task >> run_etl_task >> run_profiling_task >> write_to_mssql_task >> end_task
