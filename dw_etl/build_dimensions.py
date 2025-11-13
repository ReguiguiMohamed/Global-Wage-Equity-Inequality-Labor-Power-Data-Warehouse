from config import OUT
import pandas as pd

def build_dim_sex_age(ilostat_frames: list[pd.DataFrame]):
    # from ILO: collect distinct sex & classif1
    sex_vals, age_vals = set(), set()
    for df in ilostat_frames:
        if "sex" in df.columns: sex_vals |= set(df["sex"].dropna().unique())
        if "classif1" in df.columns: age_vals |= set([v for v in df["classif1"].dropna().unique() if v and v.lower()!="total"])
    
    # Add a placeholder for 'Not Applicable' or 'Total' if the sets are not empty
    if sex_vals:
        sex_vals.add("Not Applicable")
    if age_vals:
        age_vals.add("Not Applicable")

    dim_sex = pd.DataFrame({"sex_key": range(len(sex_vals)), "sex": sorted(list(sex_vals))})
    dim_age = pd.DataFrame({"age_key": range(len(age_vals)), "age_group": sorted(list(age_vals))})

    if not dim_sex.empty:
        dim_sex.to_csv(OUT["DIM_SEX"], index=False)
        print(f"✓ Dimension 'Dim_Sex' built with {len(dim_sex)} values.")
    if not dim_age.empty:
        dim_age.to_csv(OUT["DIM_AGE"], index=False)
        print(f"✓ Dimension 'Dim_Age' built with {len(dim_age)} values.")
        
    return dim_sex, dim_age
