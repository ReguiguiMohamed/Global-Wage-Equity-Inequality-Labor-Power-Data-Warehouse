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

    # --- Fact_IncomeDistribution ---
    fact_income_distribution = merge_data([wiid, owid_top10], on=["iso3", "country_name", "year"])
    if not fact_income_distribution.empty:
        profiles.append(write_fact(OUT["FACT_INCOME_DISTRIBUTION"], fact_income_distribution, "Fact_IncomeDistribution"))

    # --- Fact_LabourForce & Fact_Earnings ---
    labour_force_dfs = [df for name, df in ilos.items() if name != 'EAR_4MTH_SEX_CUR_NB_A']
    earnings_df = ilos.get('EAR_4MTH_SEX_CUR_NB_A')

    fact_labour_force = merge_data(labour_force_dfs, on=["iso3", "country_name", "year", "sex", "age_group"])
    if not fact_labour_force.empty:
        profiles.append(write_fact(OUT["FACT_LABOUR_FORCE"], fact_labour_force, "Fact_LabourForce"))

    if earnings_df is not None and not earnings_df.empty:
        profiles.append(write_fact(OUT["FACT_EARNINGS"], earnings_df, "Fact_Earnings"))

    # --- Fact_SocialDevelopment ---
    fact_social_development = merge_data([undp, wb_lit], on=["iso3", "country_name", "year"])
    if not fact_social_development.empty:
        profiles.append(write_fact(OUT["FACT_SOCIAL_DEVELOPMENT"], fact_social_development, "Fact_SocialDevelopment"))

    # --- Fact_EconomicState ---
    fact_economic_state = merge_data([wb_pov, owid_gov], on=["iso3", "country_name", "year"])
    if not fact_economic_state.empty:
        profiles.append(write_fact(OUT["FACT_ECONOMIC_STATE"], fact_economic_state, "Fact_EconomicState"))

    return "\n\n".join(profiles)
