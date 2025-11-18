import pandas as pd
from config import OUT
from utils import profile_block


def create_economy_fact_table(
    name: str,
    data_sources: list,
    dim_geography: pd.DataFrame,
    dim_time: pd.DataFrame,
    dim_sex: pd.DataFrame,
    dim_age: pd.DataFrame,
    dim_indicator: pd.DataFrame,
    dim_economic_classification: pd.DataFrame,
) -> pd.DataFrame | None:
    """
    Build the economy fact table from ILOSTAT sources.

    Keys:
      - geography_key, time_key, sex_key, age_key, economic_classification_key, indicator_key
    Measures:
      - value (one row per indicator / country / year / sex / age band)
    """
    if not data_sources:
        return None

    processed_dfs = []
    for df in data_sources:
        key_cols = ["iso3", "country_name", "year", "sex", "age_group"]
        measure_cols = [c for c in df.columns if c not in key_cols]
        if not measure_cols:
            continue
        measure_col = measure_cols[0]

        renamed_df = df.rename(columns={measure_col: "value"})
        renamed_df["indicator_code"] = measure_col
        processed_dfs.append(renamed_df)

    if not processed_dfs:
        return None

    long_df = pd.concat(processed_dfs, ignore_index=True)
    long_df.dropna(subset=["value"], inplace=True)

    # Geography and economic classification (by income group)
    geo_cols = ["iso3", "geography_key", "income_group"]
    long_df = pd.merge(
        long_df,
        dim_geography[geo_cols],
        on="iso3",
        how="left",
    )
    long_df = pd.merge(
        long_df,
        dim_economic_classification[
            ["income_group", "economic_classification_key"]
        ],
        on="income_group",
        how="left",
    )

    # Time
    long_df = pd.merge(
        long_df,
        dim_time[["year", "time_key"]],
        on="year",
        how="left",
    )

    # Indicator
    long_df = pd.merge(
        long_df,
        dim_indicator[["indicator_code", "indicator_key"]],
        on="indicator_code",
        how="left",
    )

    # Sex and age dimensions
    dim_sex_simplified = dim_sex[["sex_label", "sex_key"]]
    long_df = pd.merge(
        long_df,
        dim_sex_simplified,
        left_on="sex",
        right_on="sex_label",
        how="left",
    )

    dim_age_simplified = dim_age[["age_group", "age_key"]]
    long_df = pd.merge(
        long_df,
        dim_age_simplified,
        on="age_group",
        how="left",
    )

    fact_table = long_df[
        [
            "geography_key",
            "time_key",
            "sex_key",
            "age_key",
            "economic_classification_key",
            "indicator_key",
            "value",
        ]
    ]

    write_fact(OUT[name], fact_table, name)
    return fact_table


def create_fact_table(
    name: str,
    data_sources: list,
    dim_geography: pd.DataFrame,
    dim_time: pd.DataFrame,
    dim_indicator: pd.DataFrame,
    dim_source: pd.DataFrame,
    dim_economic_classification: pd.DataFrame,
) -> pd.DataFrame | None:
    """
    Build a generic fact table from wide country/year data sources.

    Used for:
      - Inequality fact
      - Social development fact

    Keys:
      - geography_key, time_key, economic_classification_key, source_key, unit_key, indicator_key
    Measures:
      - value (one row per indicator / country / year)
    """
    if not data_sources:
        return None

    all_long_dfs = []
    for df in data_sources:
        if df is None or df.empty:
            continue

        id_vars = [c for c in ["iso3", "year", "country_name"] if c in df.columns]
        if not id_vars:
            continue

        melted_df = df.melt(
            id_vars=id_vars,
            var_name="indicator_code",
            value_name="value",
        )
        all_long_dfs.append(melted_df)

    if not all_long_dfs:
        return None

    long_df = pd.concat(all_long_dfs, ignore_index=True)
    long_df.dropna(subset=["value"], inplace=True)

    # Geography (incl. income group) and economic classification
    geo_cols = ["iso3", "geography_key", "income_group"]
    long_df = pd.merge(
        long_df,
        dim_geography[geo_cols],
        on="iso3",
        how="left",
    )
    long_df = pd.merge(
        long_df,
        dim_economic_classification[
            ["income_group", "economic_classification_key"]
        ],
        on="income_group",
        how="left",
    )

    # Time
    long_df = pd.merge(
        long_df,
        dim_time[["year", "time_key"]],
        on="year",
        how="left",
    )

    # Indicator, source
    long_df = pd.merge(
        long_df,
        dim_indicator[["indicator_code", "indicator_key", "unit", "source"]],
        on="indicator_code",
        how="left",
    )
    long_df = pd.merge(
        long_df,
        dim_source[["source_code", "source_key"]],
        left_on="source",
        right_on="source_code",
        how="left",
    )

    # For these facts, sex/age are not meaningful – keep schema lean
    fact_table = long_df[
        [
            "geography_key",
            "time_key",
            "economic_classification_key",
            "source_key",
            "indicator_key",
            "value",
        ]
    ]

    write_fact(OUT[name], fact_table, name)
    return fact_table


def write_fact(path, df, name: str) -> str:
    """Write a fact table to CSV and return its profile block."""
    df.to_csv(path, index=False)
    print(f"Fact '{name}' built with {len(df)} records.")
    return profile_block(df, name)


def build_and_write_facts(
    dim_geography: pd.DataFrame,
    dim_sex: pd.DataFrame,
    dim_age: pd.DataFrame,
    dim_time: pd.DataFrame,
    dim_indicator: pd.DataFrame,
    dim_source: pd.DataFrame,
    dim_economic_classification: pd.DataFrame,
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
    owid_caloric_cv: pd.DataFrame,
    owid_gov: pd.DataFrame,
) -> str:
    """
    Build and persist all fact tables, returning a combined profiling string.
    """
    profiles: list[str] = []

    # --- Fact_Economy ---
    fact_economy_sources = list(ilos.values())
    fact_economy = create_economy_fact_table(
        "FACT_ECONOMY",
        fact_economy_sources,
        dim_geography,
        dim_time,
        dim_sex,
        dim_age,
        dim_indicator,
        dim_economic_classification,
    )
    if fact_economy is not None:
        profiles.append(profile_block(fact_economy, "Fact_Economy"))

    # --- Fact_Inequality ---
    fact_inequality_sources = [wiid, owid_top10, owid_top1, wb_pov]
    fact_inequality = create_fact_table(
        "FACT_INEQUALITY",
        fact_inequality_sources,
        dim_geography,
        dim_time,
        dim_indicator,
        dim_source,
        dim_economic_classification,
    )
    if fact_inequality is not None:
        profiles.append(profile_block(fact_inequality, "Fact_Inequality"))

    # --- Fact_SocialDevelopment ---
    # Merge former "policy" measures (minimum wage, gov spending) into Social Development
    fact_social_development_sources = [
        undp,
        wb_lit,
        owid_life_expectancy,
        owid_education_inequality,
        owid_caloric_cv,      # ← new
        min_wage,
        owid_gov,
    ]
    fact_social_development = create_fact_table(
        "FACT_SOCIAL_DEVELOPMENT",
        fact_social_development_sources,
        dim_geography,
        dim_time,
        dim_indicator,
        dim_source,
        dim_economic_classification,
    )
    if fact_social_development is not None:
        profiles.append(
            profile_block(fact_social_development, "Fact_SocialDevelopment")
        )

    return "\n\n".join(profiles)

