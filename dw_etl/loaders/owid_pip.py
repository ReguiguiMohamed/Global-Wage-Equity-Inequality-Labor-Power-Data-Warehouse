import pandas as pd
from config import DATA_DIR, FILES
from utils import exclude_israel

def load_pip_top1() -> pd.DataFrame:
    """
    Loads the OWID/PIP Top 1% income share data.

    Returns:
        A DataFrame with iso3, country_name, year, and top_1_percent_share.
    """
    csv_name = "income-share-top-1-before-tax-wid.csv"
    df = pd.read_csv(DATA_DIR / csv_name)
    
    # Rename columns
    df.rename(columns={"Code":"iso3","Entity":"country_name", "Year": "year", "Top 1% - Share (Pretax) (Estimated)": "top_1_percent_share"}, inplace=True)
    
    # Type conversions
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["top_1_percent_share"] = pd.to_numeric(df["top_1_percent_share"], errors="coerce")
    
    # Exclude countries and drop rows with no value
    df = exclude_israel(df, "iso3")
    df.dropna(subset=["year", "top_1_percent_share"], inplace=True)

    # Select and reorder columns
    final_cols = ["iso3", "country_name", "year", "top_1_percent_share"]
    return df[final_cols]

def load_pip_top10() -> pd.DataFrame:
    """
    Loads the OWID/PIP Top 10% income share data.

    Returns:
        A DataFrame with iso3, country_name, year, and top_10_percent_share.
    """
    csv_name = FILES["OWID_PIP_TOP10"]
    df = pd.read_csv(DATA_DIR / csv_name)
    
    # Rename columns
    df.rename(columns={"Code":"iso3","Entity":"country_name", "Year": "year"}, inplace=True)
    
    # Try to find a numeric measure column and rename it
    value_col = None
    for c in ["top10share","share_of_income_held_by_top_10_percent","value", "Share (Richest decile, 2021 prices)"]:
        if c in df.columns:
            value_col = c; break
    if value_col is None:
        # fallback: last column that is not year, iso3, country_name
        non_measure_cols = ['iso3', 'country_name', 'year']
        value_col = [c for c in df.columns if c not in non_measure_cols][-1]

    df.rename(columns={value_col: "top_10_percent_share"}, inplace=True)

    # Type conversions
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["top_10_percent_share"] = pd.to_numeric(df["top_10_percent_share"], errors="coerce")
    
    # Exclude countries and drop rows with no value
    df = exclude_israel(df, "iso3")
    df.dropna(subset=["year", "top_10_percent_share"], inplace=True)

    # Select and reorder columns
    final_cols = ["iso3", "country_name", "year", "top_10_percent_share"]
    return df[final_cols]
