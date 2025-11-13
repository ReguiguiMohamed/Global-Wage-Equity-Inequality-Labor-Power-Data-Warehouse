import pandas as pd
from config import DATA_DIR
from utils import to_numeric_series, exclude_israel

def load_ilostat_quick(csv_name: str, measure_name: str) -> pd.DataFrame:
    """
    Loads a single ILOSTAT "quick download" file and transforms it.
    
    Args:
        csv_name: The name of the CSV file in the data directory.
        measure_name: The desired name for the measure column (e.g., 'unemployment_rate').

    Returns:
        A DataFrame with iso3, country_name, year, sex, age_group, and the specific measure.
    """
    df = pd.read_csv(DATA_DIR / csv_name, dtype=str)
    
    # Define columns to keep and their new names
    rename_map = {
        "ref_area.label": "country_name",
        "sex.label": "sex",
        "classif1.label": "age_group",
        "time": "year",
        "obs_value": measure_name
    }
    keep_cols = [k for k in rename_map.keys() if k in df.columns]
    
    df = df[keep_cols].copy()
    df.rename(columns=rename_map, inplace=True)

    # Type conversions
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df[measure_name] = to_numeric_series(df[measure_name])

    # Add missing dimension columns for conformity
    if "iso3" not in df.columns:
        df["iso3"] = None # Will be harmonized later
    if "sex" not in df.columns:
        df["sex"] = "Not Applicable"
    if "age_group" not in df.columns:
        df["age_group"] = "Not Applicable"

    # Exclude countries and drop rows with no value
    df = exclude_israel(df, "iso3")
    df.dropna(subset=["year", measure_name], inplace=True)

    # Select and reorder columns
    final_cols = ["iso3", "country_name", "year", "sex", "age_group", measure_name]
    return df[[c for c in final_cols if c in df.columns]]
