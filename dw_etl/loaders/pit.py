import pandas as pd
from config import DATA_DIR
from utils import exclude_israel

def load_pit_rates(dim_country: pd.DataFrame) -> pd.DataFrame:
    """
    Loads the Personal Income Tax (PIT) rates data.

    Args:
        dim_country: The country dimension DataFrame for harmonization.

    Returns:
        A DataFrame with iso3, country_name, pit_rate.
    """
    csv_name = "cleaned_Personal income tax (PIT) rates.csv"
    df = pd.read_csv(DATA_DIR / csv_name, header=None, names=["country_name", "pit_rate"])
    
    # Harmonize country names and merge with dim_country to get iso3
    df = pd.merge(df, dim_country[['country_name', 'iso3']], on='country_name', how='left')

    # Type conversions
    df["pit_rate"] = pd.to_numeric(df["pit_rate"], errors="coerce")
    
    # Exclude countries and drop rows with no value
    df = exclude_israel(df, "iso3")
    df.dropna(subset=["pit_rate"], inplace=True)

    # Select and reorder columns
    final_cols = ["iso3", "country_name", "pit_rate"]
    return df[[c for c in final_cols if c in df.columns]]
