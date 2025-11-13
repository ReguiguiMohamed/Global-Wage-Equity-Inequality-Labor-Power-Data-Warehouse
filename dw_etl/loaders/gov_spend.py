import pandas as pd
from config import DATA_DIR, FILES
from utils import exclude_israel

def load_gov_spend() -> pd.DataFrame:
    """
    Loads the OWID Government Spending data.

    Returns:
        A DataFrame with iso3, country_name, year, and gov_spending_gdp_percent.
    """
    csv_name = FILES["OWID_GOV_SPEND"]
    df = pd.read_csv(DATA_DIR / csv_name)
    
    # Rename columns
    df.rename(columns={"Code":"iso3","Entity":"country_name", "Year": "year"}, inplace=True)
    
    # Try to find a numeric measure column and rename it
    value_col = None
    for c in ["government_spending_share_of_gdp","value","spending_percent_gdp", "Government expenditure (% of GDP)"]:
        if c in df.columns:
            value_col = c; break
    if value_col is None:
        # fallback: last column that is not year, iso3, country_name
        non_measure_cols = ['iso3', 'country_name', 'year']
        value_col = [c for c in df.columns if c not in non_measure_cols][-1]

    df.rename(columns={value_col: "gov_spending_gdp_percent"}, inplace=True)

    # Type conversions
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["gov_spending_gdp_percent"] = pd.to_numeric(df["gov_spending_gdp_percent"], errors="coerce")
    
    # Exclude countries and drop rows with no value
    df = exclude_israel(df, "iso3")
    df.dropna(subset=["year", "gov_spending_gdp_percent"], inplace=True)

    # Select and reorder columns
    final_cols = ["iso3", "country_name", "year", "gov_spending_gdp_percent"]
    return df[final_cols]
