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
    dim_time: pd.DataFrame,
    wiid: pd.DataFrame,
    ilos: dict,
    min_wage: pd.DataFrame,
    wb_lit: pd.DataFrame,
    wb_pov: pd.DataFrame,
    undp: pd.DataFrame,
    owid_top10: pd.DataFrame,
    owid_top1: pd.DataFrame,
    owid_life_expectancy: pd.DataFrame,
    owid_education_inequality: pd.DataFrame,
    owid_gov: pd.DataFrame,
    pit_rates: pd.DataFrame
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

    def merge_with_time(df: pd.DataFrame, dim_time: pd.DataFrame) -> pd.DataFrame:
        """Merges a dataframe with the time dimension."""
        if 'year' not in df.columns:
            return df
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df = df.dropna(subset=['year'])
        df['year'] = df['year'].astype(int)
        
        # Merge and replace year with time_key
        merged_df = pd.merge(df, dim_time[['year', 'time_key']], on='year', how='left')
        return merged_df.drop(columns=['year'])

    # --- Fact_Economy ---
    fact_economy = merge_data(list(ilos.values()), on=["iso3", "country_name", "year", "sex", "age_group"])
    if not fact_economy.empty:
        fact_economy = merge_with_time(fact_economy, dim_time)
        profiles.append(write_fact(OUT["FACT_ECONOMY"], fact_economy, "Fact_Economy"))

    # --- Fact_Inequality ---
    fact_inequality = merge_data([wiid, owid_top10, owid_top1, wb_pov], on=["iso3", "country_name", "year"])
    if not fact_inequality.empty:
        fact_inequality = merge_with_time(fact_inequality, dim_time)
        profiles.append(write_fact(OUT["FACT_INEQUALITY"], fact_inequality, "Fact_Inequality"))

    # --- Fact_SocialDevelopment ---
    fact_social_development = merge_data([undp, wb_lit, owid_life_expectancy, owid_education_inequality], on=["iso3", "country_name", "year"])
    if not fact_social_development.empty:
        fact_social_development = merge_with_time(fact_social_development, dim_time)
        profiles.append(write_fact(OUT["FACT_SOCIAL_DEVELOPMENT"], fact_social_development, "Fact_SocialDevelopment"))

    # --- Fact_Policy ---
    # owid_gov and min_wage have 'year', pit_rates does not.
    # Merge owid_gov and min_wage first, then merge pit_rates
    fact_policy_time_series = merge_data([owid_gov, min_wage], on=["iso3", "country_name", "year"])
    if not fact_policy_time_series.empty:
        fact_policy_time_series = merge_with_time(fact_policy_time_series, dim_time)
    
    # Merge pit_rates separately, as it's not time-series
    if not pit_rates.empty:
        if fact_policy_time_series.empty:
            fact_policy = pit_rates
        else:
            fact_policy = pd.merge(fact_policy_time_series, pit_rates, on=["iso3", "country_name"], how='outer')
    else:
        fact_policy = fact_policy_time_series

    if not fact_policy.empty:
        profiles.append(write_fact(OUT["FACT_POLICY"], fact_policy, "Fact_Policy"))

    return "\n\n".join(profiles)
