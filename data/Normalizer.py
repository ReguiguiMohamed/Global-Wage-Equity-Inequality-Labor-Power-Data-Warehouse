# This script prepares Step 1–2 (Explore + Schema design) for the user’s files.
# It standardizes the ILO + WIID columns into staging tables, profiles coverage,
# and emits conformed dimension seeds + suggested SQL DDL. Adjust the CONFIG paths
# to your local filenames, then run with: `python etl_step1_2.py`

import os
import re
from pathlib import Path
import pandas as pd
import numpy as np

# -----------------------------
# CONFIG: edit these file names
# -----------------------------
CONFIG = {
    # ILO "National" quick downloads (CSV)
    "ILO_FILES": {
        # informal employment rate (by sex)
        "EMP_NIFL_SEX_RT_A": "EMP_NIFL_SEX_RT_A-20251108T1840.csv",
        # unemployment rate (by sex, age)
        "UNE_DEAP_SEX_AGE_RT_A": "UNE_DEAP_SEX_AGE_RT_A-20251108T1840.csv",
        # employment-to-population ratio (by sex, age)
        "EMP_DWAP_SEX_AGE_RT_A": "EMP_DWAP_SEX_AGE_RT_A-20251108T1840.csv",
        # monthly employee earnings (by sex; classif1 varies by dataset version)
        "EAR_4MTH_SEX_CUR_NB_A": "EAR_4MTH_SEX_CUR_NB_A-20251108T1839.csv",
    },
    # WIID Companion - country level Excel
    "WIID_COUNTRY_XLSX": "wiidcountry_4.xlsx",
    # Optional: WIID global (not used here, but left for reference)
    "WIID_GLOBAL_XLSX": "wiidglobal_2.xlsx",

    # Output directory (created if absent)
    "OUT_DIR": "dw_outputs"
}

OUT_DIR = Path(CONFIG["OUT_DIR"])
OUT_DIR.mkdir(parents=True, exist_ok=True)

def load_ilo_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    # Normalize expected columns and types
    # Keep only the columns we need (if present)
    keep_cols = [
        "ref_area.label",
        "source.label",
        "indicator.label",
        "sex.label",
        "classif1.label",
        "time",
        "obs_value",
        "obs_status.label",
        "note_classif.label",
        "note_indicator.label",
        "note_source.label"
    ]
    existing = [c for c in keep_cols if c in df.columns]
    df = df[existing].copy()

    # Rename to a common schema (staging)
    rename_map = {
        "ref_area.label": "country_name",
        "source.label": "source",
        "indicator.label": "indicator",
        "sex.label": "sex",
        "classif1.label": "classif1",
        "time": "year",
        "obs_value": "value",
        "obs_status.label": "obs_status",
        "note_classif.label": "note_classif",
        "note_indicator.label": "note_indicator",
        "note_source.label": "note_source",
    }
    df.rename(columns=rename_map, inplace=True)

    # Coerce year/value types when possible
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    if "value" in df.columns:
        # Remove thousands separators, ensure numeric
        df["value"] = (
            df["value"]
            .str.replace(",", "", regex=False)
            .str.replace("\u00a0", "", regex=False)
        )
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Strip whitespace
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].str.strip()

    return df

def profile_df(df: pd.DataFrame, name: str) -> str:
    lines = [f"### {name}"]
    lines.append(f"- rows: {len(df):,}")
    if "year" in df.columns:
        years = df["year"].dropna()
        if not years.empty:
            lines.append(f"- years: {int(years.min())}–{int(years.max())} (distinct={years.nunique():,})")
    if "country_name" in df.columns:
        lines.append(f"- countries: {df['country_name'].dropna().nunique():,}")
    # Null percentage quick check
    nulls = df.isna().mean().sort_values(ascending=False)
    top_nulls = nulls.head(5)
    lines.append("- top null ratios: " + ", ".join([f"{c}={v:.1%}" for c, v in top_nulls.items()]))
    return "\n".join(lines)

def load_wiid_country(path: Path) -> pd.DataFrame:
    # WIID Companion 'country' workbook (sheet name can vary; read all and concat)
    xls = pd.ExcelFile(path)
    sheets = xls.sheet_names
    dfs = []
    for s in sheets:
        df = pd.read_excel(path, sheet_name=s)
        dfs.append(df)
    wiid = pd.concat(dfs, ignore_index=True)
    # Normalize column names to lower snake-ish
    wiid.columns = [str(c).strip().lower() for c in wiid.columns]
    return wiid

def build_dim_pays_from_wiid(wiid: pd.DataFrame) -> pd.DataFrame:
    # Expect columns: country, c3 (iso3), region_wb, region_un, region_un_sub, incomegroup, population, gdp, year
    cols = ["country","c3","region_wb","region_un","region_un_sub","incomegroup","population","gdp","year"]
    for c in cols:
        if c not in wiid.columns:
            wiid[c] = np.nan

    # Most recent population per country
    wiid_sorted = wiid.sort_values(["country","year"], ascending=[True, False])
    latest = wiid_sorted.groupby(["country","c3"], as_index=False).first()
    dim = latest[["country","c3","region_wb","region_un","region_un_sub","incomegroup","population","gdp"]].copy()
    dim.rename(columns={
        "country":"country_name",
        "c3":"iso3",
        "region_wb":"region_wb",
        "region_un":"region_un",
        "region_un_sub":"region_un_sub",
        "incomegroup":"income_group",
        "population":"population_latest",
        "gdp":"gdp_latest"
    }, inplace=True)
    # Deduplicate by iso3, favor the first (most recent)
    dim = dim.drop_duplicates(subset=["iso3"], keep="first")
    # Sort for readability
    dim = dim.sort_values("country_name").reset_index(drop=True)
    return dim

def harmonize_countries(ilo_df: pd.DataFrame, dim_pays: pd.DataFrame, source_name: str) -> pd.DataFrame:
    m = ilo_df.merge(dim_pays[["iso3","country_name"]].rename(columns={"country_name":"country_name_dim"}),
                     left_on="country_name", right_on="country_name_dim", how="left")
    m["iso3"] = m["iso3"].astype("string")
    # Report unmatched
    unmatched = m[m["iso3"].isna()]["country_name"].dropna().unique().tolist()
    if unmatched:
        with open(OUT_DIR / f"UNMATCHED_COUNTRIES_{source_name}.txt","w", encoding="utf-8") as f:
            f.write("\n".join(sorted(unmatched)))
    # prefer dim name
    m["country_name"] = np.where(m["country_name_dim"].notna(), m["country_name_dim"], m["country_name"])
    m.drop(columns=["country_name_dim"], inplace=True)
    return m

def choose_wiid_gini_variant(wiid: pd.DataFrame) -> pd.DataFrame:
    # Prefer standardized series when available; fall back to gini.
    # Indicators per WIID Companion docs:
    # - giniseries==1 -> standardized (use gini_std)
    # - shareseries==1 -> distribution-based series (use gini, palma, s80s20, etc.)
    wiid2 = wiid.copy()
    # ensure presence
    for c in ["giniseries","shareseries","gini_std","gini","palma","s80s20"]:
        if c not in wiid2.columns:
            wiid2[c] = np.nan

    std = wiid2[wiid2["giniseries"]==1].copy()
    if std.empty:
        # fallback to shareseries
        std = wiid2[wiid2["shareseries"]==1].copy()

    keep_cols = ["country","c3","year","gini_std","gini","palma","s80s20"]
    for c in keep_cols:
        if c not in std.columns:
            std[c] = np.nan
    std = std[keep_cols].copy()
    std.rename(columns={"country":"country_name","c3":"iso3"}, inplace=True)
    return std

def build_dims_from_ilo(ilos: dict) -> dict:
    # Extract distinct values for Sex, Age (classif1 containing age patterns), Sector (elsewhere)
    dim_sex = []
    dim_age = []
    dim_sector = []
    for name, df in ilos.items():
        if "sex" in df.columns:
            dim_sex.extend(df["sex"].dropna().unique().tolist())
        if "classif1" in df.columns:
            vals = df["classif1"].dropna().unique().tolist()
            for v in vals:
                s = str(v)
                if re.search(r"\b(age|years|15-|15–|^\d{2}-\d{2}$)", s, flags=re.IGNORECASE):
                    dim_age.append(s)
                else:
                    dim_sector.append(s)
    dim_sex = sorted(set(dim_sex))
    dim_age = sorted(set(dim_age))
    dim_sector = sorted(set([x for x in dim_sector if x.strip() and x.strip().lower() not in {"total","all","none"}]))

    D = {}
    D["Dim_Sex"] = pd.DataFrame({"sex_key": range(1, len(dim_sex)+1), "sex": dim_sex})
    if dim_age:
        D["Dim_Age"] = pd.DataFrame({"age_key": range(1, len(dim_age)+1), "age_group": dim_age})
    if dim_sector:
        D["Dim_Sector"] = pd.DataFrame({"sector_key": range(1, len(dim_sector)+1), "sector": dim_sector})
    return D

def save_profile_report(sections: list, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Data Profiling Report (Step 1)\n\n")
        for sec in sections:
            f.write(sec + "\n\n")

def write_sql_ddl(dim_pays: pd.DataFrame, dims_other: dict, path: Path):
    ddl = []
    ddl.append("""-- === DIMENSIONS ===
CREATE TABLE Dim_Pays (
    iso3 CHAR(3) PRIMARY KEY,
    country_name VARCHAR(100),
    region_wb VARCHAR(100),
    region_un VARCHAR(100),
    region_un_sub VARCHAR(100),
    income_group VARCHAR(50),
    population_latest BIGINT NULL,
    gdp_latest DECIMAL(18,2) NULL
);

CREATE TABLE Dim_Temps (
    year INT PRIMARY KEY,
    quarter TINYINT NULL,
    month TINYINT NULL,
    decade INT NULL
);

CREATE TABLE Dim_Sex (
    sex_key INT IDENTITY(1,1) PRIMARY KEY,
    sex VARCHAR(20)
);

CREATE TABLE Dim_Age (
    age_key INT IDENTITY(1,1) PRIMARY KEY,
    age_group VARCHAR(50)
);

CREATE TABLE Dim_Sector (
    sector_key INT IDENTITY(1,1) PRIMARY KEY,
    sector VARCHAR(100)
);

CREATE TABLE Dim_Indicateur (
    indicator_key INT IDENTITY(1,1) PRIMARY KEY,
    indicator_name VARCHAR(120),
    unit VARCHAR(40),
    source VARCHAR(80)
);

-- === FACTS ===
CREATE TABLE Fact_Inequality (
    iso3 CHAR(3) NOT NULL REFERENCES Dim_Pays(iso3),
    year INT NOT NULL REFERENCES Dim_Temps(year),
    gini_std DECIMAL(10,4) NULL,
    gini DECIMAL(10,4) NULL,
    palma DECIMAL(10,4) NULL,
    s80s20 DECIMAL(10,4) NULL,
    CONSTRAINT PK_Fact_Inequality PRIMARY KEY (iso3, year)
);

CREATE TABLE Fact_LabourEquity (
    iso3 CHAR(3) NOT NULL REFERENCES Dim_Pays(iso3),
    year INT NOT NULL REFERENCES Dim_Temps(year),
    sex_key INT NULL REFERENCES Dim_Sex(sex_key),
    age_key INT NULL REFERENCES Dim_Age(age_key),
    sector_key INT NULL REFERENCES Dim_Sector(sector_key),
    indicator_key INT NOT NULL REFERENCES Dim_Indicateur(indicator_key),
    value DECIMAL(18,6) NULL,
    CONSTRAINT PK_Fact_LabourEquity PRIMARY KEY (iso3, year, sex_key, age_key, sector_key, indicator_key)
);
""")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(ddl))

# -----------------------------
# MAIN EXECUTION
# -----------------------------
sections = []

# Load WIID country
wiid_path = Path(CONFIG["WIID_COUNTRY_XLSX"])
if wiid_path.exists():
    wiid = load_wiid_country(wiid_path)
    sections.append(profile_df(wiid, f"WIID Country ({wiid_path.name})"))
else:
    wiid = pd.DataFrame()
    sections.append(f"### WIID Country: file not found at {wiid_path}")

# Build Dim_Pays
if not wiid.empty:
    dim_pays = build_dim_pays_from_wiid(wiid)
    dim_pays.to_csv(OUT_DIR / "Dim_Pays_seed.csv", index=False)
else:
    dim_pays = pd.DataFrame(columns=["iso3","country_name"])

# Choose WIID inequality variant
if not wiid.empty:
    fact_wiid_preview = choose_wiid_gini_variant(wiid)
    fact_wiid_preview.to_csv(OUT_DIR / "STAGING_WIID_Inequality.csv", index=False)
    sections.append(profile_df(fact_wiid_preview, "STAGING_WIID_Inequality"))
else:
    sections.append("### STAGING_WIID_Inequality: skipped (no WIID loaded)")

# Load ILO files
ilo_staging = {}
for code, fname in CONFIG["ILO_FILES"].items():
    p = Path(fname)
    if p.exists():
        df = load_ilo_csv(p)
        sections.append(profile_df(df, f"ILO raw staging: {code}"))
        # Harmonize country names to ISO3 using Dim_Pays
        df2 = harmonize_countries(df, dim_pays, code)
        # Attach indicator_code so we can later map to Dim_Indicateur
        df2["indicator_code"] = code
        df2.to_csv(OUT_DIR / f"STAGING_{code}.csv", index=False)
        ilo_staging[code] = df2
    else:
        sections.append(f"### ILO file missing: {code} → {fname}")

# Build dims from ILO distincts
dims_other = build_dims_from_ilo(ilo_staging)
for name, d in dims_other.items():
    d.to_csv(OUT_DIR / f"{name}.csv", index=False)
    sections.append(profile_df(d, name))

# Build Dim_Temps range from WIID + ILO years
years = []
if not wiid.empty and "year" in wiid.columns:
    years.extend(wiid["year"].dropna().astype(int).tolist())
for df in ilo_staging.values():
    if "year" in df.columns:
        years.extend(df["year"].dropna().astype(int).tolist())
if years:
    y_min, y_max = min(years), max(years)
    dim_temps = pd.DataFrame({"year": list(range(y_min, y_max+1))})
    dim_temps["decade"] = (dim_temps["year"] // 10) * 10
    dim_temps.to_csv(OUT_DIR / "Dim_Temps.csv", index=False)
    sections.append(profile_df(dim_temps, "Dim_Temps"))
else:
    sections.append("### Dim_Temps: no year values detected")

# Indicator lookup seed
indicator_rows = [
    ("Unemployment rate", "%", "ILOSTAT", "UNE_DEAP_SEX_AGE_RT_A"),
    ("Employment-to-population ratio", "%", "ILOSTAT", "EMP_DWAP_SEX_AGE_RT_A"),
    ("Informal employment rate", "%", "ILOSTAT", "EMP_NIFL_SEX_RT_A"),
    ("Monthly employee earnings", "currency (nominal)", "ILOSTAT", "EAR_4MTH_SEX_CUR_NB_A"),
    ("Gini coefficient (standardized)", "index 0–100", "WIID", "WIID_gini_std"),
    ("Gini coefficient", "index 0–100", "WIID", "WIID_gini"),
    ("Palma ratio", "ratio", "WIID", "WIID_palma"),
    ("S80/S20 income share ratio", "ratio", "WIID", "WIID_s80s20"),
]
dim_indic = pd.DataFrame(indicator_rows, columns=["indicator_name","unit","source","indicator_code"])
dim_indic.to_csv(OUT_DIR / "Dim_Indicateur_seed.csv", index=False)
sections.append(profile_df(dim_indic, "Dim_Indicateur_seed"))

# Save profiling report
save_profile_report(sections, OUT_DIR / "profiling_report.md")

# Write SQL DDL
write_sql_ddl(dim_pays, dims_other, OUT_DIR / "dw_ddl_step2.sql")

# Final note: list outputs
sorted([str(p) for p in OUT_DIR.iterdir()])