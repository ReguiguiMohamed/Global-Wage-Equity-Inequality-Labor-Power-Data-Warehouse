import shutil
import time
from pathlib import Path

from config import FILES, OUT, OUT_DIR
from loaders.wiid import load_wiid_country
from loaders.ilostat import load_ilostat_quick, load_ilostat_minimum_wage
from loaders.worldbank import load_worldbank_wide
from loaders.undp_hdi import load_hdi_csv
from loaders.owid_pip import load_pip_top10, load_pip_top1
from loaders.owid_social import load_owid_life_expectancy, load_owid_education_inequality, load_owid_caloric_cv
from loaders.gov_spend import load_gov_spend
from build_geography_dimension import build_dim_geography
from build_dimensions import build_dim_sex_age
from build_time_dimension import build_dim_time
from build_indicator_dimension import build_dim_indicator
from build_source_dimension import build_dim_source
from build_economic_classification_dimension import build_dim_economic_classification
from build_facts import build_and_write_facts
from utils import profile_block


# Mapping for ILO files to their new measure names
ILO_MEASURE_MAP = {
    "UNE_DEAP_SEX_AGE_RT_A": "unemployment_rate",
    "EMP_DWAP_SEX_AGE_RT_A": "employment_to_population_ratio",
    "EAP_DWAP_SEX_AGE_RT_A": "labour_force_participation_rate",
    "EMP_NIFL_SEX_RT_A": "informal_employment_rate",
    "EIP_NEET_SEX_RT_A": "youth_neet_rate",
    "EAR_4MTH_SEX_CUR_NB_A": "avg_monthly_earnings",
}


def main() -> None:
    """Orchestrate the entire ETL process for the warehouse."""
    print("--- Starting GWEILPDW ETL ---")

    # === Cleanup ===
    if OUT_DIR.exists():
        print(f"--- Deleting existing output directory: {OUT_DIR} ---")
        time.sleep(2)  # Allow time for file locks to be released
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(exist_ok=True)

    # === Dimensions that depend only on raw data ===
    print("\n--- Building Geography Dimension ---")
    dim_geography = build_dim_geography()

    # === Extract & Transform ===
    print("\n--- Loading and Transforming Source Data ---")
    wiid = load_wiid_country()
    print(f"[INFO] Loaded WIID data with {len(wiid)} records.")

    ilos = {}
    for file_key, fname in FILES["ILO"].items():
        if file_key in ILO_MEASURE_MAP:
            measure_name = ILO_MEASURE_MAP[file_key]
            ilos[file_key] = load_ilostat_quick(fname, measure_name, dim_geography)
            print(
                f"[INFO] Loaded ILOSTAT '{fname}' "
                f"({measure_name}) with {len(ilos[file_key])} records."
            )

    min_wage = load_ilostat_minimum_wage(
        FILES["ILO"]["EAR_4MMN_CUR_NB_A"], "minimum_wage", dim_geography
    )
    print(f"[INFO] Loaded ILOSTAT Minimum Wage data with {len(min_wage)} records.")

    wb_lit = load_worldbank_wide(
        FILES["WB_LITERACY"], "literacy_rate", dim_geography
    )
    print(f"[INFO] Loaded World Bank Literacy data with {len(wb_lit)} records.")

    wb_pov = load_worldbank_wide(
        FILES["WB_POVERTY"], "gini", dim_geography, indicator_name="Gini index"
    )
    print(f"[INFO] Loaded World Bank Poverty data with {len(wb_pov)} records.")

    undp = load_hdi_csv(dim_geography)
    print(f"[INFO] Loaded UNDP HDI data with {len(undp)} records.")

    owid_top10 = load_pip_top10()
    print(f"[INFO] Loaded OWID Top 10% Share data with {len(owid_top10)} records.")

    owid_top1 = load_pip_top1()
    print(f"[INFO] Loaded OWID Top 1% Share data with {len(owid_top1)} records.")

    owid_life_expectancy = load_owid_life_expectancy()
    print(
        f"[INFO] Loaded OWID Life Expectancy data "
        f"with {len(owid_life_expectancy)} records."
    )

    owid_education_inequality = load_owid_education_inequality()
    print(
        f"[INFO] Loaded OWID Education Inequality data "
        f"with {len(owid_education_inequality)} records."
    )

    owid_caloric_cv = load_owid_caloric_cv()
    print(f"[INFO] Loaded OWID Caloric CV data with {len(owid_caloric_cv)} records.")

    owid_gov = load_gov_spend()
    print(f"[INFO] Loaded OWID Government Spending data with {len(owid_gov)} records.")

    # --- Build other dimensions from loaded / derived data ---
    print("\n--- Building Core Dimensions ---")
    dim_sex, dim_age = build_dim_sex_age()
    dim_time = build_dim_time()
    dim_indicator = build_dim_indicator()

    print("\n--- Building Additional Dimensions (Source, Econ) ---")
    dim_source = build_dim_source()
    dim_economic_classification = build_dim_economic_classification(dim_geography)

    # === Facts ===
    print("\n--- Building and Writing Fact Tables ---")
    profile_text = build_and_write_facts(
        dim_geography=dim_geography,
        dim_sex=dim_sex,
        dim_age=dim_age,
        dim_time=dim_time,
        dim_indicator=dim_indicator,
        dim_source=dim_source,
        dim_economic_classification=dim_economic_classification,
        wiid=wiid,
        ilos=ilos,
        min_wage=min_wage,
        wb_lit=wb_lit,
        wb_pov=wb_pov,
        undp=undp,
        owid_top10=owid_top10,
        owid_top1=owid_top1,
        owid_life_expectancy=owid_life_expectancy,
        owid_education_inequality=owid_education_inequality,
        owid_caloric_cv=owid_caloric_cv,
        owid_gov=owid_gov,
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
        "OWID_Top1": owid_top1,
        "OWID_GovSpend": owid_gov,
    }
    blocks = [profile_block(df, name) for name, df in all_dfs.items()]
    blocks.append("\n# Fact Table Profiles\n" + profile_text)

    Path(OUT["PROFILE"]).write_text("\n\n".join(blocks), encoding="utf-8")
    print(f"[INFO] Profiling report saved to {OUT['PROFILE']}")

    print("\n--- ETL Process Completed Successfully ---")
    print(f"All outputs are in the '{OUT_DIR.resolve()}' directory.")


if __name__ == "__main__":
    main()

