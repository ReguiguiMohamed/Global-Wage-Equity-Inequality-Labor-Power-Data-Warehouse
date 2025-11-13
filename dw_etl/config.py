from pathlib import Path

# Root paths
DATA_DIR = Path("./data")
OUT_DIR  = Path("./out")
OUT_DIR.mkdir(exist_ok=True)

# Policy choices
EXCLUDE_ISO3 = {"ISR"}

# File names
FILES = {
    # WIID
    "WIID_COUNTRY_XLSX": "wiidcountry_4.xlsx",
    "WIID_GLOBAL_XLSX": "wiidglobal_2.xlsx", # For Dim_Country

    # ILOSTAT
    "ILO": {
        "UNE_DEAP_SEX_AGE_RT_A": "UNE_DEAP_SEX_AGE_RT_A-20251112T2214.csv",
        "EMP_DWAP_SEX_AGE_RT_A": "EMP_DWAP_SEX_AGE_RT_A-20251112T2214.csv",
        "EMP_NIFL_SEX_RT_A":     "EMP_NIFL_SEX_RT_A-20251112T2216.csv",
        "EAR_4MTH_SEX_CUR_NB_A": "EAR_4MTH_SEX_CUR_NB_A-20251112T2214.csv",
        "EAP_DWAP_SEX_AGE_RT_A": "EAP_DWAP_SEX_AGE_RT_A-20251112T2214.csv",
        "EIP_NEET_SEX_RT_A":     "EIP_NEET_SEX_RT_A-20251112T2324.csv",
    },

    # World Bank
    "WB_LITERACY": "API_SE.ADT.LITR.ZS_DS2_en_csv_v2_216048.csv",
    "WB_POVERTY":  "API_11_DS2_en_csv_v2_126266.csv",
    
    # UNDP
    "UNDP_HDI_CSV": "HDR25_Statistical_Annex_HDI_Table.csv",

    # Other
    "OWID_PIP_TOP10": "income-share-of-the-top-10-pip.csv",
    "OWID_GOV_SPEND": "historical-gov-spending-gdp.csv",
}

# Output file names
OUT = {
    # Dimensions
    "DIM_COUNTRY": OUT_DIR / "Dim_Country.csv",
    "DIM_SEX":     OUT_DIR / "Dim_Sex.csv",
    "DIM_AGE":     OUT_DIR / "Dim_Age.csv",

    # Facts
    "FACT_INCOME_DISTRIBUTION": OUT_DIR / "Fact_IncomeDistribution.csv",
    "FACT_LABOUR_FORCE":        OUT_DIR / "Fact_LabourForce.csv",
    "FACT_EARNINGS":            OUT_DIR / "Fact_Earnings.csv",
    "FACT_SOCIAL_DEVELOPMENT":  OUT_DIR / "Fact_SocialDevelopment.csv",
    "FACT_ECONOMIC_STATE":      OUT_DIR / "Fact_EconomicState.csv",

    # Other
    "PROFILE": OUT_DIR / "profiling_report.md",
}
