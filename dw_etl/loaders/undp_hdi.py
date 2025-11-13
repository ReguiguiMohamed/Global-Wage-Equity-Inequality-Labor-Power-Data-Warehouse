import pandas as pd
from config import DATA_DIR, FILES
from utils import exclude_israel

def load_hdi_csv(dim_country: pd.DataFrame) -> pd.DataFrame:
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
    
    # Harmonize country names and merge with dim_country to get iso3
    df['country_name'] = df['country_name'].replace({"Bolivia (Plurinational State of)": "Bolivia", "Congo (Democratic Republic of the)": "Congo, Dem. Rep.", "Congo (Republic of the)": "Congo, Rep.", "Côte d'Ivoire": "Cote d'Ivoire", "Egypt": "Egypt, Arab Rep.", "Gambia": "Gambia, The", "Hong Kong, China (SAR)": "Hong Kong SAR, China", "Iran (Islamic Republic of)": "Iran, Islamic Rep.", "Korea (Republic of)": "Korea, Rep.", "Kyrgyzstan": "Kyrgyz Republic", "Lao People's Democratic Republic": "Lao PDR", "Micronesia (Federated States of)": "Micronesia, Fed. Sts.", "Moldova (Republic of)": "Moldova", "Russian Federation": "Russia", "Slovakia": "Slovak Republic", "Syrian Arab Republic": "Syria", "Tanzania (United Republic of)": "Tanzania", "Türkiye": "Turkey", "United Kingdom": "United Kingdom", "United States": "United States", "Venezuela (Bolivarian Republic of)": "Venezuela, RB", "Viet Nam": "Vietnam"})
    df = pd.merge(df, dim_country[['country_name', 'iso3']], on='country_name', how='left')

    # Exclude countries by name if possible (iso3 is not available here)
    df = df[~df['country_name'].str.lower().isin(['israel'])].copy()

    # Select and reorder columns
    final_cols = ["iso3", "country_name", "year", "hdi"]
    return df[final_cols]
