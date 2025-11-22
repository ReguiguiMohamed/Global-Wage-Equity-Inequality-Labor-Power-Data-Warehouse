from config import OUT
import pandas as pd

def build_dim_sex_age():
    """
    Builds static Dim_Gender and Dim_Age tables.
    """
    # Dim_Gender (was Dim_Sex)
    gender_data = {
        "gender_code": ["T", "M", "F"],
        "gender_label": ["Total", "Male", "Female"]
    }
    dim_gender = pd.DataFrame(gender_data)
    dim_gender["gender_key"] = dim_gender.index
    dim_gender.to_csv(OUT["DIM_SEX"], index=False)  # Still output to same file for now
    print(f"[INFO] Dimension 'Dim_Gender' built with {len(dim_gender)} values.")

    # Dim_Age
    age_data = {
        "age_group": ["Total", "15-24", "25-54", "55+"],
        "age_category": ["Total", "Youth", "Prime", "Senior"]
    }
    dim_age = pd.DataFrame(age_data)
    dim_age["age_key"] = dim_age.index
    dim_age.to_csv(OUT["DIM_AGE"], index=False)
    print(f"[INFO] Dimension 'Dim_Age' built with {len(dim_age)} values.")

    return dim_gender, dim_age
