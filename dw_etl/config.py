from pathlib import Path

# Root paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR  = PROJECT_ROOT / "out"
OUT_DIR.mkdir(exist_ok=True)

# Policy choices
EXCLUDE_ISO3 = {"ISR"}

# File names
FILES = {
    # WIID
    "WIID_COUNTRY_XLSX": "wiidcountry_4.xlsx",
    "WIID_GLOBAL_CSV": "wiidglobal_2.csv", # For Dim_Geography

    # ILOSTAT
    "ILO": {
        "UNE_DEAP_SEX_AGE_RT_A": "UNE_DEAP_SEX_AGE_RT_A-20251112T2214.csv",
        "EMP_DWAP_SEX_AGE_RT_A": "EMP_DWAP_SEX_AGE_RT_A-20251112T2214.csv",
        "EMP_NIFL_SEX_RT_A":     "EMP_NIFL_SEX_RT_A-20251112T2216.csv",
        "EAR_4MTH_SEX_CUR_NB_A": "EAR_4MTH_SEX_CUR_NB_A-20251112T2214.csv",
        "EAP_DWAP_SEX_AGE_RT_A": "EAP_DWAP_SEX_AGE_RT_A-20251112T2214.csv",
        "EIP_NEET_SEX_RT_A":     "EIP_NEET_SEX_RT_A-20251112T2324.csv",
        "EAR_4MMN_CUR_NB_A":     "EAR_4MMN_CUR_NB_A-20251113T2047.csv",
    },

    # World Bank
    "WB_LITERACY": "API_SE.ADT.LITR.ZS_DS2_en_csv_v2_216048.csv",
    "WB_POVERTY":  "API_11_DS2_en_csv_v2_126266.csv",
    
    # UNDP
    "UNDP_HDI_CSV": "HDR25_Statistical_Annex_HDI_Table.csv",

    # Other
    "OWID_PIP_TOP1": "income-share-top-1-before-tax-wid.csv",
    "OWID_PIP_TOP10": "income-share-of-the-top-10-pip.csv",
    "OWID_LIFE_EXPECTANCY": "inequality-in-life-expectancy-vs-health-expenditure-per-capita.csv",
    "OWID_EDUCATION_INEQUALITY": "inequality-in-education.csv",
    "OWID_GOV_SPEND": "historical-gov-spending-gdp.csv",
    "OWID_CALORIC_CV": "coefficient-of-variation-cv-in-per-capita-caloric-intake.csv",
}

# Output file names
OUT = {
    # Dimensions
    "DIM_SEX":     OUT_DIR / "Dim_Sex.csv",
    "DIM_AGE":     OUT_DIR / "Dim_Age.csv",
    "DIM_TIME":    OUT_DIR / "Dim_Time.csv",
    "DIM_GEOGRAPHY": OUT_DIR / "Dim_Geography.csv",
    "DIM_INDICATOR": OUT_DIR / "Dim_Indicator.csv",
    "DIM_SOURCE": OUT_DIR / "Dim_Source.csv",
    "DIM_ECONOMIC_CLASSIFICATION": OUT_DIR / "Dim_Economic_Classification.csv",
    "DIM_SECTOR": OUT_DIR / "Dim_Sector.csv",
    "DIM_UNIT_OF_MEASURE": OUT_DIR / "Dim_Unit_of_Measure.csv",

    # Facts
    "FACT_ECONOMY":             OUT_DIR / "Fact_Economy.csv",
    "FACT_INEQUALITY":          OUT_DIR / "Fact_Inequality.csv",
    "FACT_SOCIAL_DEVELOPMENT":  OUT_DIR / "Fact_SocialDevelopment.csv",

    # Other
    "PROFILE": OUT_DIR / "profiling_report.md",
}
