import pandas as pd
from config import DATA_DIR, FILES
from utils import exclude_israel

def load_hdi_csv() -> pd.DataFrame:
    """
    Loads the UNDP HDI CSV file and transforms it.
    This loader is highly specific to the messy structure of the CSV version of the HDI table.
    """
    csv_name = FILES["UNDP_HDI_CSV"]
    
    # The header is complex. Data starts at row 8 (0-indexed is 7).
    try:
        df = pd.read_csv(DATA_DIR / csv_name, skiprows=7)
    except FileNotFoundError:
        print(f"⚠️  UNDP HDI file not found at {DATA_DIR / csv_name}. Skipping.")
        return pd.DataFrame()

    # Based on inspection, the relevant columns are the 2nd and 3rd.
    # Unnamed: 1 -> Country
    # Unnamed: 2 -> HDI Value
    df = df.iloc[:, [1, 2]].copy()
    df.columns = ["country_name", "hdi"]

    # The data is for a single year (2023, as per the header in the file).
    df['year'] = 2023

    # Data cleaning
    df["hdi"] = pd.to_numeric(df["hdi"], errors="coerce")
    df.dropna(subset=["hdi", "country_name"], inplace=True)
    
    # This file does not have iso3, so we will have to rely on country name harmonization later.
    df["iso3"] = None

    # Exclude countries by name if possible (iso3 is not available here)
    df = df[~df['country_name'].str.lower().isin(['israel'])].copy()

    # Select and reorder columns
    final_cols = ["iso3", "country_name", "year", "hdi"]
    return df[final_cols]
