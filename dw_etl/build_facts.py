import pandas as pd
from functools import reduce
from config import OUT
from utils import profile_block

def write_fact(path, df, name):
    """Writes a fact table to CSV and returns a profile block."""
    df.to_csv(path, index=False)
    print(f"✓ Fact '{name}' built with {len(df)} records.")
    return profile_block(df, name)

def build_and_write_facts(
    dim_country: pd.DataFrame,
    dim_sex: pd.DataFrame,
    dim_age: pd.DataFrame,
    wiid: pd.DataFrame,
    ilos: dict,
    wb_lit: pd.DataFrame,
    wb_pov: pd.DataFrame,
    undp: pd.DataFrame,
    owid_top10: pd.DataFrame,
    owid_gov: pd.DataFrame
):
    """
    Builds and writes all fact tables.
    """
    profiles = []
    
    # Helper for merging dataframes
    def merge_data(dfs: list, on: list):
        # Filter out None or empty dataframes
        valid_dfs = [df for df in dfs if df is not None and not df.empty]
        if not valid_dfs:
            return pd.DataFrame()
        return reduce(lambda left, right: pd.merge(left, right, on=on, how='outer'), valid_dfs)

    # --- Fact_Economy ---
    fact_economy = merge_data(list(ilos.values()), on=["iso3", "country_name", "year", "sex", "age_group"])
    if not fact_economy.empty:
        profiles.append(write_fact(OUT["FACT_ECONOMY"], fact_economy, "Fact_Economy"))

    # --- Fact_Inequality ---
    fact_inequality = merge_data([wiid, owid_top10, wb_pov], on=["iso3", "country_name", "year"])
    if not fact_inequality.empty:
        profiles.append(write_fact(OUT["FACT_INEQUALITY"], fact_inequality, "Fact_Inequality"))

    # --- Fact_SocialDevelopment ---
    fact_social_development = merge_data([undp, wb_lit], on=["iso3", "country_name", "year"])
    if not fact_social_development.empty:
        profiles.append(write_fact(OUT["FACT_SOCIAL_DEVELOPMENT"], fact_social_development, "Fact_SocialDevelopment"))

    # --- Fact_Policy ---
    if not owid_gov.empty:
        profiles.append(write_fact(OUT["FACT_POLICY"], owid_gov, "Fact_Policy"))

    return "\n\n".join(profiles)
