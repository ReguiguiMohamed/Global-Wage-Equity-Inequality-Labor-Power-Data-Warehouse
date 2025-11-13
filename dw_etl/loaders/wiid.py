import pandas as pd
import numpy as np
from config import DATA_DIR, FILES
from utils import exclude_israel

def load_wiid_country() -> pd.DataFrame:
    """
    Loads and transforms WIID country-level data, returning a wide DataFrame
    with specific columns for each inequality measure.
    """
    xlsx_name = FILES["WIID_COUNTRY_XLSX"]
    xls = pd.ExcelFile(DATA_DIR / xlsx_name)
    frames = [pd.read_excel(DATA_DIR / xlsx_name, sheet_name=s) for s in xls.sheet_names]
    wiid = pd.concat(frames, ignore_index=True)
    wiid.columns = [str(c).strip().lower() for c in wiid.columns]

    for c in ["country","c3","year","giniseries","shareseries","gini_std","gini","palma","s80s20"]:
        if c not in wiid.columns: wiid[c] = np.nan

    sub = wiid[wiid["giniseries"] == 1].copy()
    if sub.empty:
        sub = wiid[wiid["shareseries"] == 1].copy()

    sub.rename(columns={"country":"country_name","c3":"iso3"}, inplace=True)
    sub["year"] = pd.to_numeric(sub["year"], errors="coerce").astype("Int64")
    
    measure_cols = ["gini_std","gini","palma","s80s20"]
    for c in measure_cols:
        sub[c] = pd.to_numeric(sub[c], errors="coerce")

    sub = exclude_israel(sub, "iso3")

    # Keep it wide, just select the columns we need
    final_cols = ["iso3", "country_name", "year"] + measure_cols
    df = sub[final_cols].copy()
    
    # Drop rows where all measure columns are null
    df.dropna(subset=measure_cols, how='all', inplace=True)

    return df
