import pandas as pd
from config import DATA_DIR
from utils import year_columns, exclude_israel

def load_worldbank_wide(csv_name: str, measure_name: str, dim_country: pd.DataFrame, indicator_name: str = None) -> pd.DataFrame:
    """
    Loads a "wide" World Bank CSV and transforms it into a long format with a specific measure.

    Args:
        csv_name: The name of the CSV file in the data directory.
        measure_name: The desired name for the measure column (e.g., 'literacy_rate').
        dim_country: The country dimension DataFrame for harmonization.
        indicator_name: The name of the indicator to filter by.

    Returns:
        A DataFrame with iso3, country_name, year, and the specific measure.
    """
    raw = pd.read_csv(DATA_DIR / csv_name, skiprows=4)
    raw.rename(columns={
        "Country Code":"iso3",
        "Country Name":"country_name",
        "Indicator Name": "indicator_name"
    }, inplace=True)

    if indicator_name:
        raw = raw[raw['indicator_name'] == indicator_name].copy()

    # Filter to only countries in our dimension
    raw = raw[raw['iso3'].isin(dim_country['iso3'])].copy()

    years = year_columns(raw)
    long = raw.melt(
        id_vars=["iso3","country_name"],
        value_vars=years,
        var_name="year",
        value_name=measure_name
    )

    if measure_name == 'gini':
        long.rename(columns={'gini': 'gini_wb'}, inplace=True)
        measure_name = 'gini_wb'
    
    # Type conversions
    long["year"]  = pd.to_numeric(long["year"], errors="coerce").astype("Int64")
    long[measure_name] = pd.to_numeric(long[measure_name], errors="coerce")
    
    # Exclude countries and drop rows with no value
    long = exclude_israel(long, "iso3")
    long.dropna(subset=["year", measure_name], inplace=True)

    # Select and reorder columns
    final_cols = ["iso3", "country_name", "year", measure_name]
    return long[final_cols]
