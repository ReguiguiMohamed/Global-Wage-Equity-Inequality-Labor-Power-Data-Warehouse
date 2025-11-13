import pandas as pd
from config import DATA_DIR
from utils import exclude_israel

def load_owid_life_expectancy() -> pd.DataFrame:
    """
    Loads the OWID 'Inequality in life expectancy vs health expenditure per capita' data.

    Returns:
        A DataFrame with iso3, country_name, year, inequality_life_expectancy, and health_expenditure_per_capita.
    """
    csv_name = "inequality-in-life-expectancy-vs-health-expenditure-per-capita.csv"
    df = pd.read_csv(DATA_DIR / csv_name)
    
    # Rename columns
    df.rename(columns={
        "Code":"iso3",
        "Entity":"country_name",
        "Year": "year",
        "Inequality in life expectancy": "inequality_life_expectancy",
        "Current health expenditure per capita, PPP (current international $)": "health_expenditure_per_capita"
    }, inplace=True)
    
    # Type conversions
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["inequality_life_expectancy"] = pd.to_numeric(df["inequality_life_expectancy"], errors="coerce")
    df["health_expenditure_per_capita"] = pd.to_numeric(df["health_expenditure_per_capita"], errors="coerce")
    
    # Exclude countries and drop rows with no value
    df = exclude_israel(df, "iso3")
    df.dropna(subset=["year", "inequality_life_expectancy"], inplace=True) # Assuming inequality_life_expectancy is the primary measure
    
    # Select and reorder columns
    final_cols = ["iso3", "country_name", "year", "inequality_life_expectancy", "health_expenditure_per_capita"]
    return df[[c for c in final_cols if c in df.columns]]

def load_owid_education_inequality() -> pd.DataFrame:
    """
    Loads the OWID 'Inequality in education' data.

    Returns:
        A DataFrame with iso3, country_name, year, inequality_education.
    """
    csv_name = "inequality-in-education.csv"
    df = pd.read_csv(DATA_DIR / csv_name)
    
    # Rename columns
    df.rename(columns={
        "Code":"iso3",
        "Entity":"country_name",
        "Year": "year",
        "Inequality in education": "inequality_education"
    }, inplace=True)
    
    # Type conversions
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["inequality_education"] = pd.to_numeric(df["inequality_education"], errors="coerce")
    
    # Exclude countries and drop rows with no value
    df = exclude_israel(df, "iso3")
    df.dropna(subset=["year", "inequality_education"], inplace=True)
    
    # Select and reorder columns
    final_cols = ["iso3", "country_name", "year", "inequality_education"]
    return df[[c for c in final_cols if c in df.columns]]
