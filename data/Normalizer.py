# -*- coding: utf-8 -*-
"""
ETL Step 1–2 (with ETD integration)
- Atomic facts: Fact_InequalityMeasure, Fact_LabourMarket, Fact_PolicyEconomy
- Conformed dims (Pays/Temps/Sex/Age/Sector/Indicateur/Unit/Source)
- Israel excluded, smart ISO3 matching, WB region aggregates
- Outputs: CSVs + markdown profiling (no SQL)
"""

import os, re, unicodedata, difflib
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np

# -----------------------------
# CONFIG
# -----------------------------
CONFIG = {
    "ILO_FILES": {
        "EMP_NIFL_SEX_RT_A": "EMP_NIFL_SEX_RT_A-20251108T1840.csv",
        "UNE_DEAP_SEX_AGE_RT_A": "UNE_DEAP_SEX_AGE_RT_A-20251108T1840.csv",
        "EMP_DWAP_SEX_AGE_RT_A": "EMP_DWAP_SEX_AGE_RT_A-20251108T1840.csv",
        "EAR_4MTH_SEX_CUR_NB_A": "EAR_4MTH_SEX_CUR_NB_A-20251108T1839.csv",
    },
    "WIID_COUNTRY_XLSX": "wiidcountry_4.xlsx",
    "ETD_FILE": "ETD_230918.csv",                         # NEW
    "OUT_DIR": "dw_outputs_with_etd",
}

OUT_DIR = Path(CONFIG["OUT_DIR"])
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Helpers
# -----------------------------
def fold(s: str) -> str:
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
    "bolivia plurinational state":"bolivia","brunei darussalam":"brunei",
    "congo democratic republic of the":"democratic republic of the congo",
    "congo dem rep":"democratic republic of the congo","congo republic of the":"congo",
    "iran islamic republic of":"iran","lao people s democratic republic":"laos",
    "micronesia federated states of":"micronesia","moldova republic of":"moldova",
    "korea republic of":"south korea","korea democratic people s republic of":"north korea",
    "syrian arab republic":"syria","tanzania united republic of":"tanzania",
    "united states of america":"united states","eswatini":"swaziland",
    "cabo verde":"cape verde","holy see":"vatican city","myanmar":"burma",
}

def is_israel(name: str, iso3: str) -> bool:
    n = fold(name)
    # Safely check if iso3 is a non-NA value before comparing it.
    # This avoids the "boolean value of NA is ambiguous" error.
    is_iso_match = pd.notna(iso3) and str(iso3).upper() == "ISR"
    return is_iso_match or ("israel" in n)

def profile_df(df: pd.DataFrame, name: str) -> str:
    lines = [f"### {name}", f"- rows: {len(df):,}"]
    if "year" in df.columns:
        yrs = df["year"].dropna()
        if not yrs.empty:
            lines.append(f"- years: {int(yrs.min())}–{int(yrs.max())} (distinct={yrs.nunique():,})")
    for col in ("country_name","iso3","region_wb"):
        if col in df.columns: lines.append(f"- distinct {col}: {df[col].dropna().nunique():,}")
    if len(df.columns):
        nulls = df.isna().mean().sort_values(ascending=False)
        lines.append("- top null ratios: " + ", ".join([f"{c}={v:.1%}" for c,v in nulls.head(5).items()]))
    return "\n".join(lines)

def write_report(sections: List[str], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Data Profiling Report (Step 1 – with ETD)\n\n")
        for s in sections: f.write(s + "\n\n")

# -----------------------------
# Loaders
# -----------------------------
def load_ilo_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    keep = ["ref_area.label","source.label","indicator.label","sex.label","classif1.label",
            "time","obs_value","obs_status.label","note_classif.label","note_indicator.label","note_source.label"]
    df = df[[c for c in keep if c in df.columns]].copy()
    df.rename(columns={
        "ref_area.label":"country_name","source.label":"source","indicator.label":"indicator",
        "sex.label":"sex","classif1.label":"classif1","time":"year","obs_value":"value",
        "obs_status.label":"obs_status","note_classif.label":"note_classif",
        "note_indicator.label":"note_indicator","note_source.label":"note_source"
    }, inplace=True)
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    if "value" in df.columns:
        df["value"] = (df["value"].str.replace(",", "", regex=False).str.replace("\u00a0","",regex=False))
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    for c in df.columns:
        if df[c].dtype == object: df[c] = df[c].str.strip()
    return df

def load_wiid_country(path: Path) -> pd.DataFrame:
    if not path.exists(): return pd.DataFrame()
    xls = pd.ExcelFile(path)
    dfs = [pd.read_excel(path, sheet_name=s) for s in xls.sheet_names]
    wiid = pd.concat(dfs, ignore_index=True)
    wiid.columns = [str(c).strip().lower() for c in wiid.columns]
    return wiid

# -----------------------------
# Dims
# -----------------------------
def build_dim_pays_from_wiid(wiid: pd.DataFrame) -> pd.DataFrame:
    for c in ["country","c3","region_wb","region_un","region_un_sub","incomegroup","population","gdp","year"]:
        if c not in wiid.columns: wiid[c] = np.nan
    latest = wiid.sort_values(["country","year"], ascending=[True, False]).groupby(["country","c3"], as_index=False).first()
    dim = latest[["country","c3","region_wb","region_un","region_un_sub","incomegroup","population","gdp"]].copy()
    dim.rename(columns={"country":"country_name","c3":"iso3","incomegroup":"income_group",
                        "population":"population_latest","gdp":"gdp_latest"}, inplace=True)
    dim = dim[~dim.apply(lambda r: is_israel(r.get("country_name",""), r.get("iso3","")), axis=1)]
    dim = dim.dropna(subset=["iso3"]).drop_duplicates("iso3").sort_values("country_name").reset_index(drop=True)
    return dim

def build_country_lookup(dim_pays: pd.DataFrame) -> pd.DataFrame:
    lk = dim_pays.copy()
    lk["name_fold"] = lk["country_name"].apply(fold)
    rows = []
    for _, r in lk.iterrows():
        base = re.sub(r"\b(republic|state|islamic|federation|democratic|people s)\b","", r["name_fold"])
        base = re.sub(r"\s+"," ", base).strip()
        if base and base != r["name_fold"]:
            rows.append({"iso3":r["iso3"], "country_name":r["country_name"], "name_fold":base})
    lk = pd.concat([lk, pd.DataFrame(rows)], ignore_index=True)
    alias_rows = []
    for _, r in dim_pays.iterrows():
        canon = fold(r["country_name"])
        for k,v in ALIASES.items():
            if v == canon:
                alias_rows.append({"iso3":r["iso3"], "country_name":r["country_name"], "name_fold":k})
    if alias_rows: lk = pd.concat([lk, pd.DataFrame(alias_rows)], ignore_index=True)
    return lk.drop_duplicates(subset=["name_fold","iso3"])

def smart_match_country(name: str, lookup: pd.DataFrame) -> Tuple[str,str]:
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

def build_dims_from_ilo(ilo: Dict[str,pd.DataFrame]) -> Dict[str,pd.DataFrame]:
    sex_vals, age_vals, sector_vals = set(), set(), set()
    for _, df in ilo.items():
        if "sex" in df.columns: sex_vals |= set(df["sex"].dropna().unique().tolist())
        if "classif1" in df.columns:
            for v in [str(x) for x in df["classif1"].dropna().unique().tolist()]:
                if re.search(r"(age|years|\d{2}\s*-\s*\d{2}|15-)", v, re.I): age_vals.add(v)
                elif fold(v) not in {"","total","all","none"}: sector_vals.add(v)
    dim_sex = pd.DataFrame({"sex_key": range(1,len(sex_vals)+1), "sex": sorted(sex_vals)}) if sex_vals else pd.DataFrame(columns=["sex_key","sex"])
    dim_age = pd.DataFrame({"age_key": range(1,len(age_vals)+1), "age_group": sorted(age_vals)}) if age_vals else pd.DataFrame(columns=["age_key","age_group"])
    dim_sector = pd.DataFrame({"sector_key": range(1,len(sector_vals)+1), "sector": sorted(sector_vals)}) if sector_vals else pd.DataFrame(columns=["sector_key","sector"])
    return {"Dim_Sex":dim_sex, "Dim_Age":dim_age, "Dim_Sector":dim_sector}

def dim_units_sources_indicators_seed() -> Tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    dim_unit = pd.DataFrame({
        "unit_key":[1,2,3,4,5],
        "unit_label":["percent","index_0_100","ratio","currency_month","value"]
    })
    dim_source = pd.DataFrame({
        "source_key":[1,2,3],
        "source_name":["ILOSTAT","WIID Companion 2025-04-29","ETD Dataset 2023-09-18"]
    })
    dim_ind = pd.DataFrame([
        (1,"Unemployment rate","UNE_DEAP_SEX_AGE_RT_A","percent",1),
        (2,"Employment-to-population ratio","EMP_DWAP_SEX_AGE_RT_A","percent",1),
        (3,"Informal employment rate","EMP_NIFL_SEX_RT_A","percent",1),
        (4,"Monthly employee earnings","EAR_4MTH_SEX_CUR_NB_A","currency_month",1),
        (5,"Gini (standardized)","WIID_gini_std","index_0_100",2),
        (6,"Gini","WIID_gini","index_0_100",2),
        (7,"Palma ratio","WIID_palma","ratio",2),
        (8,"S80/S20 income share ratio","WIID_s80s20","ratio",2),
    ], columns=["indicator_key","indicator_name","indicator_code","unit_label","source_key"])
    return dim_unit, dim_source, dim_ind

# -----------------------------
# Facts (WIID + ILO)
# -----------------------------
def choose_wiid_subset(wiid: pd.DataFrame) -> pd.DataFrame:
    for c in ["giniseries","shareseries","gini_std","gini","palma","s80s20","country","c3","year"]:
        if c not in wiid.columns: wiid[c] = np.nan
    std = wiid[wiid["giniseries"]==1].copy()
    if std.empty: std = wiid[wiid["shareseries"]==1].copy()
    keep = std[["country","c3","year","gini_std","gini","palma","s80s20"]].copy()
    keep.rename(columns={"country":"country_name","c3":"iso3"}, inplace=True)
    return keep

def build_fact_inequality_measure(sub: pd.DataFrame, dim_ind: pd.DataFrame) -> pd.DataFrame:
    melt = sub.melt(id_vars=["iso3","country_name","year"],
                    value_vars=["gini_std","gini","palma","s80s20"],
                    var_name="indicator_slug", value_name="value").dropna(subset=["value"])
    code_map = {"gini_std":"WIID_gini_std","gini":"WIID_gini","palma":"WIID_palma","s80s20":"WIID_s80s20"}
    melt["indicator_code"] = melt["indicator_slug"].map(code_map)
    ind_map  = dim_ind.set_index("indicator_code")["indicator_key"].to_dict()
    unit_map = dim_ind.set_index("indicator_code")["unit_label"].to_dict()
    src_map  = dim_ind.set_index("indicator_code")["source_key"].to_dict()
    melt["indicator_key"]  = melt["indicator_code"].map(ind_map)
    melt["unit_label"]     = melt["indicator_code"].map(unit_map)
    melt["source_key"]     = melt["indicator_code"].map(src_map)
    melt["dataset_version"]= "WIID-Comp-2025-04-29"
    melt = melt[~melt.apply(lambda r: is_israel(r["country_name"], r["iso3"]), axis=1)]
    cols = ["iso3","year","indicator_key","value","country_name","unit_label","source_key","dataset_version"]
    return melt[cols].sort_values(["iso3","year","indicator_key"]).reset_index(drop=True)

def harmonize_ilo_with_dim(ilo_df: pd.DataFrame, lookup: pd.DataFrame, indicator_code: str) -> pd.DataFrame:
    df = ilo_df.copy()
    iso3_list, canon_list = [], []
    for nm in df["country_name"]:
        iso3, canon = smart_match_country(nm, lookup)
        iso3_list.append(iso3)
        canon_list.append(canon if canon else nm)
    df["iso3"] = pd.Series(iso3_list, dtype="string")
    df["country_name"] = pd.Series(canon_list, dtype="string")
    df = df[~df.apply(lambda r: is_israel(r["country_name"], r.get("iso3","")), axis=1)]
    df["indicator_code"] = indicator_code
    return df

def build_fact_labour_market(ilo: Dict[str,pd.DataFrame], dim_ind: pd.DataFrame, dim_sex: pd.DataFrame, dim_age: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for code, df in ilo.items():
        if df.empty: continue
        tmp = df.copy()
        tmp["sex_key"] = np.where(tmp.get("sex").notna(),
                                  tmp["sex"].map({s:k for k,s in zip(dim_sex["sex_key"],dim_sex["sex"])}) if not dim_sex.empty else np.nan,
                                  np.nan)
        if "classif1" in tmp.columns and not dim_age.empty:
            age_map = {g:k for k,g in zip(dim_age["age_key"], dim_age["age_group"])}
            tmp["age_key"] = tmp["classif1"].map(age_map)
        else:
            tmp["age_key"] = np.nan

        row = dim_ind[dim_ind["indicator_code"] == code]
        if row.empty: continue
        tmp["indicator_key"]  = int(row["indicator_key"].iloc[0])
        tmp["unit_label"]     = row["unit_label"].iloc[0]
        tmp["source_key"]     = int(row["source_key"].iloc[0])
        tmp["dataset_version"]= "ILOSTAT-Quick-Download"
        frames.append(tmp[["iso3","year","sex_key","age_key","indicator_key","value","country_name","unit_label","source_key","dataset_version"]])
    if not frames:
        return pd.DataFrame(columns=["iso3","year","sex_key","age_key","indicator_key","value","country_name","unit_label","source_key","dataset_version"])
    fact = pd.concat(frames, ignore_index=True)
    fact["year"]  = pd.to_numeric(fact["year"], errors="coerce").astype("Int64")
    fact["value"] = pd.to_numeric(fact["value"], errors="coerce")
    return fact.dropna(subset=["iso3","year","indicator_key","value"]).reset_index(drop=True)

# -----------------------------
# ETD → Fact_PolicyEconomy (NEW)
# -----------------------------
def infer_unit_from_name(s: str) -> str:
    s = fold(s)
    if "%" in s or "rate" in s or "share" in s: return "percent"
    if "index" in s: return "index_0_100"
    if "gdp" in s or "usd" in s or "dollar" in s: return "value"  # keep generic; convert later if needed
    return "value"

def normalize_etd_to_long(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    # standardize columns
    df.columns = [c.strip() for c in df.columns]
    lower = {c: c.lower() for c in df.columns}
    df.rename(columns=lower, inplace=True)

    # detect id columns
    country_col = next((c for c in df.columns if any(k in c for k in ["iso3","country","economy"])), None)
    year_col    = next((c for c in df.columns if "year" in c or "time" in c), None)

    if not country_col or not year_col:
        # fallback: try 'location' and 'time_period'
        country_col = country_col or "location"
        year_col    = year_col or "time_period"

    # if already long (indicator/value present)
    if {"indicator","value"} <= set(df.columns):
        long_df = df[[country_col, year_col, "indicator", "value"]].copy()
        long_df.rename(columns={country_col:"country_raw", year_col:"year"}, inplace=True)
    else:
        # assume wide: melt all numeric columns (non id)
        id_cols = [c for c in [country_col, year_col] if c in df.columns]
        value_cols = [c for c in df.columns if c not in id_cols]
        # keep only numeric-ish columns as indicators
        for c in value_cols:
            df[c] = pd.to_numeric(df[c].str.replace(",","",regex=False), errors="coerce")
        long_df = df.melt(id_vars=id_cols, value_vars=value_cols, var_name="indicator", value_name="value")
        long_df.rename(columns={country_col:"country_raw", year_col:"year"}, inplace=True)

    long_df["year"]  = pd.to_numeric(long_df["year"], errors="coerce").astype("Int64")
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    long_df = long_df.dropna(subset=["year","value"])
    return long_df

def build_fact_policyeconomy(etd_long: pd.DataFrame, lookup: pd.DataFrame,
                              dim_ind: pd.DataFrame, dim_unit: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # add source row for ETD already present (source_key=3). Extend indicators dynamically.
    dim_ind2 = dim_ind.copy()
    existing_codes = set(dim_ind2["indicator_code"])

    # map countries
    iso3s, names = [], []
    for nm in etd_long["country_raw"]:
        iso3, canon = smart_match_country(nm, lookup)
        iso3s.append(iso3)
        names.append(canon if canon else nm)
    etd_long["iso3"] = pd.Series(iso3s, dtype="string")
    etd_long["country_name"] = pd.Series(names, dtype="string")
    etd_long = etd_long[~etd_long.apply(lambda r: is_israel(r["country_name"], r.get("iso3","")), axis=1)]

    # create indicator rows if new
    to_add = []
    for ind in sorted(etd_long["indicator"].dropna().unique().tolist()):
        code = "ETD_" + re.sub(r"[^A-Za-z0-9]+","_", ind).strip("_").upper()
        if code not in existing_codes:
            unit_label = infer_unit_from_name(ind)
            to_add.append({"indicator_key": dim_ind2["indicator_key"].max()+1 if len(dim_ind2)>0 else 100,
                           "indicator_name": ind, "indicator_code": code,
                           "unit_label": unit_label, "source_key": 3})
            # increment subsequent keys
            if to_add: 
                for i in range(len(to_add)):
                    to_add[i]["indicator_key"] = (dim_ind2["indicator_key"].max() if len(dim_ind2)>0 else 8) + i + 1
    if to_add:
        dim_ind2 = pd.concat([dim_ind2, pd.DataFrame(to_add)], ignore_index=True)

    # map to keys
    map_code_to_key  = dim_ind2.set_index("indicator_name")["indicator_key"].to_dict()
    map_name_to_unit = dim_ind2.set_index("indicator_name")["unit_label"].to_dict()
    map_name_to_code = dim_ind2.set_index("indicator_name")["indicator_code"].to_dict()

    fact = etd_long.copy()
    fact["indicator_key"]  = fact["indicator"].map(map_code_to_key)
    fact["unit_label"]     = fact["indicator"].map(map_name_to_unit)
    fact["indicator_code"] = fact["indicator"].map(map_name_to_code)
    fact["source_key"]     = 3
    fact["dataset_version"]= "ETD-2023-09-18"

    fact = fact.dropna(subset=["iso3","indicator_key","value"])
    fact = fact[["iso3","year","indicator_key","value","country_name","unit_label","source_key","dataset_version","indicator_code"]]
    return fact.reset_index(drop=True), dim_ind2

# -----------------------------
# Region aggregates
# -----------------------------
# -----------------------------
# Region aggregates
# -----------------------------
def add_region_wb_to_fact(fact: pd.DataFrame, dim_pays: pd.DataFrame) -> pd.DataFrame:
    if fact.empty: return fact
    m = fact.merge(dim_pays[["iso3","region_wb","population_latest"]], on="iso3", how="left")
    
    def _agg(g):
        # g no longer contains 'year' or 'indicator_key' due to include_groups=False
        if g["population_latest"].notna().any():
            w = g["population_latest"].fillna(0); v = g["value"].fillna(0)
            denom = w.sum(); val = (v*w).sum()/denom if denom else np.nan
        else:
            val = g["value"].mean()
            
        # FIX: Only select columns that are NOT used for grouping.
        # 'unit_label', 'source_key', 'dataset_version' should be constant within a group.
        row = g.iloc[0][["unit_label","source_key","dataset_version"]] 
        
        # The grouping keys ('year', 'indicator_key', 'region_wb') are automatically 
        # returned by pandas when using as_index=False.
        return pd.Series({"value": val, **row.to_dict()})
        
    out = m.dropna(subset=["region_wb"]).groupby(["region_wb","year","indicator_key"], as_index=False).apply(_agg, include_groups=False).reset_index()
    out.rename(columns={"region_wb":"region_wb_label"}, inplace=True)
    return out

# -----------------------------
# MAIN
# -----------------------------
def main():
    sections = []

    # ---- WIID ----
    wiid = load_wiid_country(Path(CONFIG["WIID_COUNTRY_XLSX"]))
    sections.append(profile_df(wiid, f"WIID Country ({CONFIG['WIID_COUNTRY_XLSX']})"))
    dim_pays = build_dim_pays_from_wiid(wiid)
    dim_pays.to_csv(OUT_DIR / "Dim_Pays_seed.csv", index=False)
    sections.append(profile_df(dim_pays, "Dim_Pays_seed (Israel excluded)"))

    # lookup
    lookup = build_country_lookup(dim_pays)

    # ---- ILO ----
    ilo_raw, ilo_h = {}, {}
    for code, fname in CONFIG["ILO_FILES"].items():
        p = Path(fname)
        if not p.exists():
            sections.append(f"### ILO file missing: {code} → {fname}")
            continue
        df = load_ilo_csv(p)
        sections.append(profile_df(df, f"ILO raw: {code}"))
        df2 = harmonize_ilo_with_dim(df, lookup, code)
        df2.to_csv(OUT_DIR / f"STAGING_{code}.csv", index=False)
        ilo_raw[code], ilo_h[code] = df, df2

    # ---- Dims small + time ----
    dims_small = build_dims_from_ilo(ilo_h)
    for k,d in dims_small.items():
        d.to_csv(OUT_DIR / f"{k}.csv", index=False)
        sections.append(profile_df(d, k))

    years = []
    if "year" in wiid.columns: years += wiid["year"].dropna().astype(int).tolist()
    for df in ilo_h.values():
        if "year" in df.columns: years += df["year"].dropna().astype(int).tolist()
    if years:
        dim_temps = pd.DataFrame({"year": list(range(min(years), max(years)+1))})
        dim_temps["decade"] = (dim_temps["year"] // 10) * 10
        dim_temps.to_csv(OUT_DIR / "Dim_Temps.csv", index=False)
        sections.append(profile_df(dim_temps, "Dim_Temps"))

    # ---- Seed dims
    dim_unit, dim_source, dim_ind = dim_units_sources_indicators_seed()
    dim_unit.to_csv(OUT_DIR / "Dim_Unit.csv", index=False)
    dim_source.to_csv(OUT_DIR / "Dim_Source.csv", index=False)

    # ---- WIID facts
    wiid_sub = choose_wiid_subset(wiid)
    sections.append(profile_df(wiid_sub, "WIID subset for inequality"))
    fact_ineq = build_fact_inequality_measure(wiid_sub, dim_ind)
    fact_ineq.to_csv(OUT_DIR / "Fact_InequalityMeasure.csv", index=False)
    sections.append(profile_df(fact_ineq, "Fact_InequalityMeasure"))
    ineq_reg = add_region_wb_to_fact(fact_ineq, dim_pays)
    ineq_reg.to_csv(OUT_DIR / "Fact_Inequality_RegionWB.csv", index=False)
    sections.append(profile_df(ineq_reg, "Fact_Inequality_RegionWB (aggregated)"))

    # ---- ILO facts
    fact_lab = build_fact_labour_market(ilo_h, dim_ind, dims_small.get("Dim_Sex",pd.DataFrame()), dims_small.get("Dim_Age",pd.DataFrame()))
    fact_lab.to_csv(OUT_DIR / "Fact_LabourMarket.csv", index=False)
    sections.append(profile_df(fact_lab, "Fact_LabourMarket"))
    lab_reg = add_region_wb_to_fact(fact_lab, dim_pays)
    lab_reg.to_csv(OUT_DIR / "Fact_Labour_RegionWB.csv", index=False)
    sections.append(profile_df(lab_reg, "Fact_Labour_RegionWB (aggregated)"))

    # ---- ETD → Fact_PolicyEconomy (NEW)
    etd_path = Path(CONFIG["ETD_FILE"])
    if etd_path.exists():
        etd_long = normalize_etd_to_long(etd_path)
        sections.append(profile_df(etd_long, f"ETD long staging ({etd_path.name})"))
        fact_etd, dim_ind2 = build_fact_policyeconomy(etd_long, lookup, dim_ind, dim_unit)
        dim_ind2.to_csv(OUT_DIR / "Dim_Indicateur_seed.csv", index=False)   # include ETD indicators
        fact_etd.to_csv(OUT_DIR / "Fact_PolicyEconomy.csv", index=False)
        sections.append(profile_df(fact_etd, "Fact_PolicyEconomy"))
        etd_reg = add_region_wb_to_fact(fact_etd, dim_pays)
        etd_reg.to_csv(OUT_DIR / "Fact_PolicyEconomy_RegionWB.csv", index=False)
        sections.append(profile_df(etd_reg, "Fact_PolicyEconomy_RegionWB (aggregated)"))
    else:
        # still export baseline indicator seed (no ETD)
        dim_ind.to_csv(OUT_DIR / "Dim_Indicateur_seed.csv", index=False)
        sections.append(f"### ETD file missing: {CONFIG['ETD_FILE']}")

    # ---- Unmatched diagnostics from ILO raw
    unmatched = []
    for code, df in ilo_raw.items():
        names = df["country_name"].dropna().unique().tolist()
        for nm in names:
            iso3,_ = smart_match_country(nm, build_country_lookup(dim_pays))
            if not iso3:
                cand = difflib.get_close_matches(fold(nm), build_country_lookup(dim_pays)["name_fold"].unique().tolist(), n=1, cutoff=0.75)
                suggestion = build_country_lookup(dim_pays).query("name_fold == @cand[0]")["country_name"].iloc[0] if cand else ""
                unmatched.append({"source":code,"country_name":nm,"suggested_match":suggestion})
    if unmatched:
        pd.DataFrame(unmatched).drop_duplicates().to_csv(OUT_DIR / "UNMATCHED_COUNTRIES_suggestions.csv", index=False)

    # ---- Report
    write_report(sections, OUT_DIR / "profiling_report_with_etd.md")

if __name__ == "__main__":
    pd.options.display.max_columns = 120
    main()
