from config import FILES, OUT, OUT_DIR
from loaders.wiid import load_wiid_country
from loaders.ilostat import load_ilostat_quick
from loaders.worldbank import load_worldbank_wide
from loaders.undp_hdi import load_hdi_csv
from loaders.owid_pip import load_pip_top10
from loaders.gov_spend import load_gov_spend
from build_country_dimension import build_dim_country
from build_dimensions import build_dim_sex_age
from build_facts import build_and_write_facts
from utils import profile_block
from pathlib import Path

# Mapping for ILO files to their new measure names
ILO_MEASURE_MAP = {
    "UNE_DEAP_SEX_AGE_RT_A": "unemployment_rate",
    "EMP_DWAP_SEX_AGE_RT_A": "employment_to_population_ratio",
    "EAP_DWAP_SEX_AGE_RT_A": "labour_force_participation_rate",
    "EMP_NIFL_SEX_RT_A": "informal_employment_rate",
    "EIP_NEET_SEX_RT_A": "youth_neet_rate",
    "EAR_4MTH_SEX_CUR_NB_A": "avg_monthly_earnings",
}

def main():
    """Orchestrates the entire ETL process."""
    print("--- Starting GWEILPDW ETL Refactor ---")

    # === Dimensions ===
    print("\n--- Building Dimensions ---")
    dim_country = build_dim_country()
    # We need to load all ilo files to build sex and age dimensions
    ilos_for_dims = [load_ilostat_quick(FILES["ILO"][file_key], measure) for file_key, measure in ILO_MEASURE_MAP.items()]
    dim_sex, dim_age = build_dim_sex_age(ilos_for_dims)
    
    # === Extract & Transform ===
    print("\n--- Loading and Transforming Source Data ---")
    wiid = load_wiid_country()
    print(f"✓ Loaded WIID data with {len(wiid)} records.")

    ilos = {}
    for file_key, fname in FILES["ILO"].items():
        measure_name = ILO_MEASURE_MAP[file_key]
        ilos[file_key] = load_ilostat_quick(fname, measure_name)
        print(f"✓ Loaded ILOSTAT '{fname}' ({measure_name}) with {len(ilos[file_key])} records.")

    wb_lit = load_worldbank_wide(FILES["WB_LITERACY"], "literacy_rate")
    print(f"✓ Loaded World Bank Literacy data with {len(wb_lit)} records.")
    wb_pov = load_worldbank_wide(FILES["WB_POVERTY"], "poverty_headcount_ratio")
    print(f"✓ Loaded World Bank Poverty data with {len(wb_pov)} records.")

    undp = load_hdi_csv()
    print(f"✓ Loaded UNDP HDI data with {len(undp)} records.")
    owid_top10 = load_pip_top10()
    print(f"✓ Loaded OWID Top 10% Share data with {len(owid_top10)} records.")
    owid_gov = load_gov_spend()
    print(f"✓ Loaded OWID Government Spending data with {len(owid_gov)} records.")

    # === Facts ===
    print("\n--- Building and Writing Fact Tables ---")
    profile_text = build_and_write_facts(
        dim_country=dim_country,
        dim_sex=dim_sex,
        dim_age=dim_age,
        wiid=wiid,
        ilos=ilos,
        wb_lit=wb_lit,
        wb_pov=wb_pov,
        undp=undp,
        owid_top10=owid_top10,
        owid_gov=owid_gov
    )

    # === Quick profile file ===
    print("\n--- Generating Profiling Report ---")
    all_dfs = {
        "WIID": wiid,
        **{f"ILO_{k}": v for k, v in ilos.items()},
        "WB_Literacy": wb_lit,
        "WB_Poverty": wb_pov,
        "UNDP_HDI": undp,
        "OWID_Top10": owid_top10,
        "OWID_GovSpend": owid_gov,
    }
    blocks = [profile_block(df, name) for name, df in all_dfs.items()]
    blocks.append("\n# Fact Table Profiles\n" + profile_text)
    
    Path(OUT["PROFILE"]).write_text("\n\n".join(blocks), encoding="utf-8")
    print(f"✓ Profiling report saved to {OUT['PROFILE']}")
    
    print("\n--- ETL Process Completed Successfully ---")
    print(f"All outputs are in the '{OUT_DIR.resolve()}' directory.")

if __name__ == "__main__":
    main()
