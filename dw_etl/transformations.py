import pandas as pd
import numpy as np


def map_age_group(age_series: pd.Series) -> pd.Series:
    """
    Map detailed ILOSTAT age-band labels onto the warehouse's canonical age
    groups used in Dim_Age: 'Total', '15-24', '25-54', '55+'.

    This collapses various 5-year, 10-year, and aggregate bands into those
    buckets so the ILOSTAT facts can join cleanly to Dim_Age.age_group.
    """
    age_series = age_series.astype(str)

    # Order matters: more specific numeric bands first, then catch-all totals.
    youth_pattern = r"15-19|20-24|15-24"
    adult_pattern = r"25-29|30-34|35-39|40-44|45-49|50-54|25-34|35-44|45-54|25-54"
    senior_pattern = r"55-59|60-64|65\+|55-64"
    total_pattern = r"Total|15\+|15-64"

    conditions = [
        age_series.str.contains(youth_pattern, na=False),
        age_series.str.contains(adult_pattern, na=False),
        age_series.str.contains(senior_pattern, na=False),
        age_series.str.contains(total_pattern, na=False),
    ]
    choices = ["15-24", "25-54", "55+", "Total"]

    # Default to 'Not Applicable' if no condition is met
    return pd.Series(
        np.select(conditions, choices, default="Not Applicable"),
        index=age_series.index,
    )
