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


def normalize_float_value(x):
    """
    Robustly convert any value to a valid SQL float or None.
    Handles strings, empty values, NaN, inf, and edge cases.
    """
    # Handle None and NaN first
    if x is None or (isinstance(x, float) and (pd.isna(x) or x != x)):
        return None

    # Handle empty strings or whitespace-only strings
    if isinstance(x, str):
        stripped = x.strip()
        if stripped == "" or stripped.lower() in ["", "null", "nan", "n/a", "na", "#n/a", "none"]:
            return None
        try:
            val = float(stripped)
        except (ValueError, TypeError):
            return None
    else:
        try:
            val = float(x)
        except (ValueError, TypeError):
            return None

    # Handle special float values
    if pd.isna(val) or val != val:  # NaN check
        return None
    if val == float('inf') or val == float('-inf'):
        return None

    # Check for precision/scale issues (numbers too large or with too many decimals)
    # SQL Server FLOAT has limits - be cautious with extremely large numbers
    if abs(val) > 1e38:
        print(f"Warning: Value {val} exceeds SQL Server FLOAT limits, converting to None")
        return None

    return val


def clean_dataframe_strict(df):
    """
    Aggressively clean DataFrame before sending to SQL Server.
    Focuses on identifying and converting problematic columns.
    """
    df_cleaned = df.copy()

    for col in df_cleaned.columns:
        col_lower = col.lower()

        # For numeric columns, apply strict normalization
        if any(x in col_lower for x in ['value', 'amount', 'percentage', 'rate', 'ratio', 'index', 'count']):
            print(f"Normalizing numeric column: {col}")
            df_cleaned[col] = df_cleaned[col].apply(normalize_float_value)
            # Convert to nullable float if not already
            df_cleaned[col] = df_cleaned[col].astype('object')  # Keep as object until insert

        # For text columns, ensure no invalid empty strings
        elif col_lower not in ['is_oecd', 'is_eu', 'is_g20']:  # Skip boolean columns
            # Convert completely empty strings to None
            df_cleaned[col] = df_cleaned[col].apply(
                lambda x: None if (isinstance(x, str) and x.strip() == "") else x
            )

    return df_cleaned


def validate_data_before_insert(df, table_name, schema_name):
    """
    Validate data types before attempting insert.
    Print warnings about suspicious data.
    """
    print(f"\n=== Validating data for {schema_name}.{table_name} ===")

    for col in df.columns:
        col_lower = col.lower()

        # Check for numeric columns
        if any(x in col_lower for x in ['value', 'amount', 'percentage', 'rate', 'ratio', 'index']):
            non_null_vals = df[col].dropna()
            if len(non_null_vals) > 0:
                try:
                    # Try to convert all non-null values
                    numeric_vals = pd.to_numeric(non_null_vals, errors='coerce')
                    null_count = numeric_vals.isna().sum()
                    if null_count > 0:
                        print(f"  WARNING: Column '{col}' has {null_count} values that couldn't convert to float")
                        # Show examples of problematic values
                        problem_vals = non_null_vals[numeric_vals.isna()].unique()[:5]
                        print(f"    Examples: {problem_vals.tolist()}")
                except Exception as e:
                    print(f"  ERROR validating column '{col}': {str(e)}")

    print(f"=== Validation complete ===\n")


def safe_tuple_convert(row):
    """
    Convert row to tuple, ensuring floats are Python float or None.
    This prevents pyodbc from receiving invalid types.
    """
    return tuple(
        None if (val is None or (isinstance(val, float) and (pd.isna(val) or val != val)))
        else float(val) if isinstance(val, (int, float)) and not pd.isna(val)
        else val
        for val in row
    )


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

            # For geography dimension, normalise boolean flags for BIT columns.
            # Use string '0'/'1' so SQL Server can safely CAST to BIT without
            # any ambiguity about float vs integer parameter types.
            if table_name == "Dim_Geography":
                def to_bit_str(x):
                    if x is None or pd.isna(x):
                        return None
                    if isinstance(x, bool):
                        return "1" if x else "0"
                    if isinstance(x, (int, float)):
                        return "1" if x != 0 else "0"
                    if isinstance(x, str):
                        v = x.strip().lower()
                        if v in ("true", "t", "yes", "y", "1"):
                            return "1"
                        if v in ("false", "f", "no", "n", "0"):
                            return "0"
                    return None

                for flag_col in ["is_oecd", "is_eu", "is_g20"]:
                    if flag_col in df_cleaned.columns:
                        df_cleaned[flag_col] = df_cleaned[flag_col].apply(to_bit_str)

            # For dimension tables, normalize boolean flag columns to 0/1 for BIT columns
            if schema_name == 'dim':
                # Normalize boolean flag columns to 0/1 for BIT columns
                for col in ["is_oecd", "is_eu", "is_g20"]:
                    if col in df_cleaned.columns:
                        def to_bit(x):
                            if x is None or pd.isna(x):
                                return None
                            if isinstance(x, bool):
                                return 1 if x else 0
                            if isinstance(x, (int, float)):
                                return 1 if x != 0 else 0
                            if isinstance(x, str):
                                v = x.strip().lower()
                                if v in ("true", "t", "yes", "y", "1"):
                                    return 1
                                if v in ("false", "f", "no", "n", "0"):
                                    return 0
                            # Fallback: treat anything else as NULL
                            return None

                        df_cleaned[col] = df_cleaned[col].apply(to_bit)

            # Insert data in chunks using pyodbc executemany to avoid parameter/size limits
            print(f"Starting chunked insert of {total_rows} rows into {full_table_name} with chunk size {chunk_size}...")
            for i in range(0, total_rows, chunk_size):
                chunk = df_cleaned.iloc[i:i + chunk_size]  # Use cleaned data

                # Use safe conversion instead of direct tuple()
                data_tuples = [safe_tuple_convert(row) for row in chunk.values]

                # Execute the insert for this chunk
                try:
                    cursor.executemany(insert_sql, data_tuples)
                    cnxn.commit()  # Commit after each chunk
                    print(f"Wrote rows {i + 1} to {min(i + chunk_size, total_rows)} of {total_rows} to {full_table_name}")
                except pyodbc.ProgrammingError as e:
                    print(f"ERROR inserting chunk at row {i}: {str(e)}")
                    # Print first problematic row for debugging
                    if data_tuples:
                        print(f"First row in chunk: {data_tuples[0]}")
                    raise

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

            # Step 1: Initial cleaning
            df = df.replace({pd.NA: None})
            df = df.where(pd.notnull(df), None)
            df = df.replace('', None)  # Replace empty strings with None

            # Step 2: Aggressive normalization of numeric columns
            for col in df.columns:
                col_lower = col.lower()
                if any(x in col_lower for x in ['value', 'amount', 'percentage', 'rate', 'ratio', 'index', 'count']):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].replace([float('inf'), float('-inf')], None)

            # Step 3: Validate before insert
            validate_data_before_insert(df, table_name, schema_name)

            # Step 4: Apply final cleaning
            df_cleaned = clean_dataframe_strict(df[insert_columns])

            # For dimension tables, normalize boolean flag columns to 0/1 for BIT columns
            if schema_name == 'dim':
                # Normalize boolean flag columns to 0/1 for BIT columns
                for col in ["is_oecd", "is_eu", "is_g20"]:
                    if col in df_cleaned.columns:
                        def to_bit(x):
                            if x is None or pd.isna(x):
                                return None
                            if isinstance(x, bool):
                                return 1 if x else 0
                            if isinstance(x, (int, float)):
                                return 1 if x != 0 else 0
                            if isinstance(x, str):
                                v = x.strip().lower()
                                if v in ("true", "t", "yes", "y", "1"):
                                    return 1
                                if v in ("false", "f", "no", "n", "0"):
                                    return 0
                            # Fallback: treat anything else as NULL
                            return None

                        df_cleaned[col] = df_cleaned[col].apply(to_bit)

            # Insert data in chunks using pyodbc executemany to avoid parameter/size limits
            print(f"Starting chunked insert of {total_rows} rows into {full_table_name} with chunk size {chunk_size}...")
            for i in range(0, total_rows, chunk_size):
                chunk = df_cleaned.iloc[i:i+chunk_size]  # Use cleaned data

                # Use safe conversion instead of direct tuple()
                data_tuples = [safe_tuple_convert(row) for row in chunk.values]

                # Execute the insert for this chunk
                try:
                    cursor.executemany(insert_sql, data_tuples)
                    cnxn.commit()  # Commit after each chunk
                    print(f"Wrote rows {i+1} to {min(i+chunk_size, total_rows)} of {total_rows} to {full_table_name}")
                except pyodbc.ProgrammingError as e:
                    print(f"ERROR inserting chunk at row {i}: {str(e)}")
                    # Print first problematic row for debugging
                    if data_tuples:
                        print(f"First row in chunk: {data_tuples[0]}")
                    raise

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
