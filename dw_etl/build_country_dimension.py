import pandas as pd
from pathlib import Path
from config import DATA_DIR, FILES, OUT, EXCLUDE_ISO3

def load_wiid_global(path: Path) -> pd.DataFrame:
    """Load and combine WIID global sheets."""
    if not path.exists():
        print(f"Error: WIID global file not found at {path}")
        return pd.DataFrame()
    xls = pd.ExcelFile(path)
    dfs = [pd.read_excel(path, sheet_name=s) for s in xls.sheet_names]
    wiid = pd.concat(dfs, ignore_index=True)
    wiid.columns = [str(c).strip().lower() for c in wiid.columns]
    return wiid

def build_dim_country():
    """Builds the conformed country dimension from the WIID Global file."""
    wiid = load_wiid_global(DATA_DIR / FILES["WIID_GLOBAL_XLSX"])
    if wiid.empty:
        return pd.DataFrame()

    # Select latest country info and rename columns
    cols = ["country","c3","region_wb","population","year"]
    for c in cols:
        if c not in wiid.columns: wiid[c] = pd.NA
        
    latest = wiid.sort_values(["country","year"], ascending=[True, False]).groupby(["country","c3"], as_index=False).first()
    dim = latest[["country","c3","region_wb","population"]].copy()
    dim.rename(columns={"country":"country_name","c3":"iso3", "region_wb": "world_bank_region", "population":"population_latest"}, inplace=True)
    
    # Apply exclusions and cleaning
    dim = dim[~dim["iso3"].isin(EXCLUDE_ISO3)]
    dim = dim.dropna(subset=["iso3"]).drop_duplicates("iso3").sort_values("country_name").reset_index(drop=True)
    
    dim.to_csv(OUT["DIM_COUNTRY"], index=False)
    print(f"✓ Dimension 'Dim_Country' built with {len(dim)} countries.")
    return dim
