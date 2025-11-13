import re
import unicodedata
import difflib
from pathlib import Path
from typing import Tuple
import pandas as pd
import numpy as np

# -----------------------------
# CONFIGURATION
# -----------------------------
CONFIG = {
    # REQUIRED: New file for World Bank region mapping and population data
    "WIID_REGION_MAP_XLSX": "wiidglobal_2.xlsx", 
    
    # REQUIRED: The data file to aggregate
    "ETD_FILE": "ETD_230918.csv",              
    
    "OUT_DIR": "dw_regional_etd_output_v2",
}

OUT_DIR = Path(CONFIG["OUT_DIR"])
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# 1. HELPERS & UTILITIES (Unchanged)
# -----------------------------

def fold(s: str) -> str:
    """Normalize and simplify strings for matching."""
    if s is None or (isinstance(s, float) and np.isnan(s)): return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join([c for c in s if not unicodedata.combining(c)])
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

ALIASES = {
    "cote d ivoire": "cote d'ivoire","ivory coast":"cote d'ivoire","viet nam":"vietnam",
    "russian federation":"russia","bolivia plurinational state of":"bolivia",
    "brunei darussalam":"brunei","congo democratic republic of the":"democratic republic of the congo",
    "iran islamic republic of":"iran","lao people s democratic republic":"laos",
    "micronesia federated states of":"micronesia","moldova republic of":"moldova",
    "korea republic of":"south korea","korea democratic people s republic of":"north korea",
    "syrian arab republic":"syria","tanzania united republic of":"tanzania",
    "united states of america":"united states","eswatini":"swaziland","cabo verde":"cape verde",
}

def is_israel(name: str, iso3: str) -> bool:
    """Check if a record corresponds to Israel for exclusion."""
    n = fold(name)
    is_iso_match = pd.notna(iso3) and str(iso3).upper() == "ISR"
    return is_iso_match or ("israel" in n)

def smart_match_country(name: str, lookup: pd.DataFrame) -> Tuple[str,str]:
    """Match country name to ISO3 using aliases and fuzzy matching."""
    if not name: return "",""
    nf = ALIASES.get(fold(name), fold(name))
    hit = lookup[lookup["name_fold"] == nf]
    if not hit.empty:
        row = hit.iloc[0]; return row["iso3"], row["country_name"]
    choices = lookup["name_fold"].unique().tolist()
    best = difflib.get_close_matches(nf, choices, n=1, cutoff=0.88)
    if best:
        row = lookup[lookup["name_fold"] == best[0]].iloc[0]
        return row["iso3"], row["country_name"]
    return "",""

# -----------------------------
# 2. DIMENSION BUILDERS
# -----------------------------

def load_wiid_country(path: Path) -> pd.DataFrame:
    """Load and combine WIID country sheets."""
    if not path.exists():
        print(f"Error: WIID region map file not found at {path}")
        return pd.DataFrame()
    xls = pd.ExcelFile(path)
    dfs = [pd.read_excel(path, sheet_name=s) for s in xls.sheet_names]
    wiid = pd.concat(dfs, ignore_index=True)
    wiid.columns = [str(c).strip().lower() for c in wiid.columns]
    return wiid

def build_dim_pays_from_wiid(wiid: pd.DataFrame) -> pd.DataFrame:
    """Build the core Country Dimension (Dim_Pays) for mapping and aggregation."""
    # Select latest country info and rename columns
    cols = ["country","c3","region_wb","population","year"]
    for c in cols:
        if c not in wiid.columns: wiid[c] = np.nan
        
    latest = wiid.sort_values(["country","year"], ascending=[True, False]).groupby(["country","c3"], as_index=False).first()
    dim = latest[["country","c3","region_wb","population"]].copy()
    dim.rename(columns={"country":"country_name","c3":"iso3",
                        "population":"population_latest"}, inplace=True)
    
    # Apply exclusions and cleaning
    dim = dim[~dim.apply(lambda r: is_israel(r.get("country_name",""), r.get("iso3","")), axis=1)]
    dim = dim.dropna(subset=["iso3"]).drop_duplicates("iso3").sort_values("country_name").reset_index(drop=True)
    return dim

def build_country_lookup(dim_pays: pd.DataFrame) -> pd.DataFrame:
    """Build a comprehensive lookup table for country matching (including aliases)."""
    lk = dim_pays[["country_name", "iso3"]].copy()
    lk["name_fold"] = lk["country_name"].apply(fold)
    
    # Add common aliases for robust matching
    alias_rows = []
    for _, r in dim_pays.iterrows():
        canon = fold(r["country_name"])
        for k,v in ALIASES.items():
            if v == canon:
                alias_rows.append({"iso3":r["iso3"], "country_name":r["country_name"], "name_fold":k})
    if alias_rows: lk = pd.concat([lk, pd.DataFrame(alias_rows)], ignore_index=True)
    
    return lk.drop_duplicates(subset=["name_fold","iso3"])

# -----------------------------
# 3. ETD FACT BUILDING & CLEANING
# -----------------------------

def normalize_etd_to_long(path: Path) -> pd.DataFrame:
    """Load ETD data and transform it into a long (tidy) format."""
    if not path.exists():
        print(f"Error: ETD file not found at {path}")
        return pd.DataFrame()
    
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]

    # Detect country and year columns (assuming wide format for simplicity)
    country_col = next((c for c in df.columns if any(k in c for k in ["iso3","country","economy","location"])), None)
    year_col = next((c for c in df.columns if any(k in c for k in ["year","time","time_period"])), None)

    if not country_col or not year_col:
        print("Error: Could not determine country or year column from ETD file.")
        return pd.DataFrame()

    id_cols = [c for c in [country_col, year_col] if c in df.columns]
    value_cols = [c for c in df.columns if c not in id_cols]
    
    # Convert value columns to numeric, cleaning the thousands separator
    for c in value_cols:
        df[c] = pd.to_numeric(df[c].astype(str).str.replace(",","",regex=False), errors="coerce")
        
    long_df = df.melt(id_vars=id_cols, value_vars=value_cols, var_name="indicator", value_name="value")
    long_df.rename(columns={country_col:"country_raw", year_col:"year"}, inplace=True)

    long_df["year"] = pd.to_numeric(long_df["year"], errors="coerce").astype("Int64")
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    
    return long_df.dropna(subset=["year","value"]).reset_index(drop=True)


def create_etd_fact(etd_long: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """Harmonize ETD data with ISO3 and canonical names."""
    
    # 1. Match countries
    iso3s, names = [], []
    for nm in etd_long["country_raw"]:
        iso3, canon = smart_match_country(nm, lookup)
        iso3s.append(iso3)
        names.append(canon if canon else nm)
        
    etd_long["iso3"] = pd.Series(iso3s, dtype="string")
    etd_long["country_name"] = pd.Series(names, dtype="string")
    
    # 2. Exclude Israel and drop unmatched rows
    etd_fact = etd_long[~etd_long.apply(lambda r: is_israel(r["country_name"], r.get("iso3","")), axis=1)]
    etd_fact = etd_fact.dropna(subset=["iso3"]).copy()

    # 3. Standardize fact columns (minimal set required for aggregation)
    etd_fact["indicator_key"] = etd_fact["indicator"].astype("category").cat.codes + 1 # Dynamic indicator keying
    etd_fact["unit_label"] = "value" # Placeholder unit
    etd_fact["source_key"] = 3       # Placeholder source
    etd_fact["dataset_version"] = "ETD-2023-09-18"

    cols = ["iso3","year","indicator_key","value","country_name","unit_label","source_key","dataset_version"]
    return etd_fact[cols]

# -----------------------------
# 4. REGIONAL AGGREGATION LOGIC
# -----------------------------

def add_region_wb_to_fact(fact: pd.DataFrame, dim_pays: pd.DataFrame) -> pd.DataFrame:
    """Aggregates country-level fact data to World Bank regional level."""
    if fact.empty: return fact
    
    # Merge country fact data with WB region and population data
    m = fact.merge(dim_pays[["iso3","region_wb","population_latest"]], on="iso3", how="left")
    
    def _agg(g):
        """Custom aggregation function for GroupBy.apply() - Population-Weighted Average"""
        # g no longer contains 'year' or 'indicator_key' due to include_groups=False
        
        # Check if population data is available for weighted average
        if g["population_latest"].notna().any():
            # Population-weighted average (preferred)
            w = g["population_latest"].fillna(0)
            v = g["value"].fillna(0)
            denom = w.sum()
            val = (v*w).sum()/denom if denom else np.nan
        else:
            # Simple mean if no population data available
            val = g["value"].mean()
            
        # Select non-grouping keys (must be constant within the group)
        row = g.iloc[0][["unit_label","source_key","dataset_version"]] 
        
        return pd.Series({"value": val, **row.to_dict()})
        
    # Group by Region, Year, and Indicator, applying the custom aggregation
    out = m.dropna(subset=["region_wb"]).groupby(["region_wb","year","indicator_key"], as_index=False)\
           .apply(_agg, include_groups=False).reset_index(drop=True)

    out.rename(columns={"region_wb":"region_wb_label"}, inplace=True)
    return out

# -----------------------------
# 5. MAIN EXECUTION
# -----------------------------

def run_etd_regional_etl():
    """Main function to run the ETL steps for ETD regional aggregation."""
    
    print("--- Starting ETD Regional Aggregation (using wiidglobal_2.xlsx) ---")
    
    # 1. Load Country Mapping Data (Dim_Pays)
    wiid = load_wiid_country(Path(CONFIG["WIID_REGION_MAP_XLSX"]))
    if wiid.empty: return
        
    dim_pays = build_dim_pays_from_wiid(wiid)
    lookup = build_country_lookup(dim_pays)
    print(f"Loaded {len(dim_pays):,} countries for WB region mapping.")
    
    # Optional: Display WB regions loaded
    print(f"Distinct World Bank Regions loaded: {dim_pays['region_wb'].dropna().nunique()}")

    # 2. Load and Prepare ETD Fact Data
    etd_long = normalize_etd_to_long(Path(CONFIG["ETD_FILE"]))
    if etd_long.empty: return
        
    fact_etd = create_etd_fact(etd_long, lookup)
    print(f"Prepared Fact data with {len(fact_etd):,} records for aggregation.")
    
    # 3. Aggregate to WB Regions
    etd_reg = add_region_wb_to_fact(fact_etd, dim_pays)
    
    # 4. Save Results
    output_path = OUT_DIR / "Fact_PolicyEconomy_RegionWB.csv"
    etd_reg.to_csv(output_path, index=False)
    
    print(f"\n--- SUCCESS ---")
    print(f"Output saved to: {output_path}")
    print(f"Total Regional Aggregations (Year/Indicator/Region): {len(etd_reg):,}")
    print(f"Distinct WB Regions in Output: {etd_reg['region_wb_label'].nunique()}")
    
if __name__ == "__main__":
    pd.options.display.max_columns = 120
    run_etd_regional_etl()