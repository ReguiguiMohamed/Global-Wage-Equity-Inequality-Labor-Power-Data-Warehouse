import pandas as pd

files_to_read = [
    "out/Dim_Geography.csv",
    "out/Dim_Age.csv",
    "out/Dim_Gender.csv", # Renamed from Dim_Sex.csv in the listing
    "out/Dim_Source.csv",
    "out/Dim_Sector.csv",
    "out/Dim_Unit_of_Measure.csv",
    "out/Fact_Inequality.csv",
]

for file_path in files_to_read:
    print(f"\n--- Content of {file_path} ---")
    try:
        df = pd.read_csv(file_path)
        print(df.head())
        print(f"Shape: {df.shape}")
    except FileNotFoundError:
        print(f"Error: {file_path} not found.")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
