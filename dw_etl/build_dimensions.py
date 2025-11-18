from config import OUT
import pandas as pd

def build_dim_sex_age():
    """
    Builds static Dim_Sex and Dim_Age tables.
    """
    # Dim_Sex
    sex_data = {
        "sex_code": ["T", "M", "F"],
        "sex_label": ["Total", "Male", "Female"]
    }
    dim_sex = pd.DataFrame(sex_data)
    dim_sex["sex_key"] = dim_sex.index
    dim_sex.to_csv(OUT["DIM_SEX"], index=False)
    print(f"[INFO] Dimension 'Dim_Sex' built with {len(dim_sex)} values.")

    # Dim_Age
    age_data = {
        "age_group": ["Total", "15-24", "25-54", "55+"],
        "age_category": ["Total", "Youth", "Prime", "Senior"]
    }
    dim_age = pd.DataFrame(age_data)
    dim_age["age_key"] = dim_age.index
    dim_age.to_csv(OUT["DIM_AGE"], index=False)
    print(f"[INFO] Dimension 'Dim_Age' built with {len(dim_age)} values.")
        
    return dim_sex, dim_age
