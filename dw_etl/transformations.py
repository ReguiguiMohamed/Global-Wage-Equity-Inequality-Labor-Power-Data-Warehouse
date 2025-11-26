import pandas as pd
import numpy as np


def map_age_group(age_series: pd.Series) -> pd.Series:
    """
    Map ILOSTAT age-band labels onto the warehouse's canonical age
    groups used in Dim_Age: 'Total', '15-24', '25-54', '55+'.

    IMPORTANT: from the ILOSTAT extractions we only keep the
    "Age (10-year bands)" series to avoid overlapping totals:
      - Age (10-year bands): 15-24  -> '15-24'  (Youth)
      - Age (10-year bands): 25-34,
        35-44, 45-54              -> '25-54'  (Prime / Adult)
      - Age (10-year bands): 55-64,
        65+                        -> '55+'    (Senior)
      - Age (10-year bands): Total -> 'Total'

    Any other age formulations (e.g. "Age (Aggregate bands): Total",
    "Age (Youth, adults): 15+") are set to 'Not Applicable' so they
    do not interfere with totals in downstream tools like Power BI.
    """
    age_series = age_series.astype(str)

    # Only consider the 10-year band series.
    ten_year_mask = age_series.str.contains(r"Age \(10-year bands\):", na=False)

    youth_pattern = r"15-24"
    adult_pattern = r"25-34|35-44|45-54"
    senior_pattern = r"55-64|65\+"
    total_pattern = r"Total"

    conditions = [
        ten_year_mask & age_series.str.contains(youth_pattern, na=False),
        ten_year_mask & age_series.str.contains(adult_pattern, na=False),
        ten_year_mask & age_series.str.contains(senior_pattern, na=False),
        ten_year_mask & age_series.str.contains(total_pattern, na=False),
    ]
    choices = ["15-24", "25-54", "55+", "Total"]

    # Default to 'Not Applicable' if no condition is met
    return pd.Series(
        np.select(conditions, choices, default="Not Applicable"),
        index=age_series.index,
    )
