import pandas as pd
import numpy as np

def map_age_group(age_series: pd.Series) -> pd.Series:
    """Maps detailed age group strings to simplified categories."""
    
    age_series = age_series.astype(str)
    
    # Define mapping from keywords/patterns to new categories
    # Order matters: more specific patterns should come first.
    conditions = [
        age_series.str.contains(r'15-19|20-24|25-29|15-24|<15|10-14|5-9', na=False),
        age_series.str.contains(r'30-34|35-39|40-44|45-49|50-54|55-59|25-34|35-44|45-54|25-54', na=False),
        age_series.str.contains(r'60-64|65\+|55-64', na=False),
        age_series.str.contains(r'Total|15\+|15-64|25\+', na=False)
    ]
    choices = ['Youth', 'Adults', 'Elderly', 'Total']
    
    # Default to 'Not Applicable' if no condition is met
    return pd.Series(np.select(conditions, choices, default='Not Applicable'), index=age_series.index)
