import pandas as pd
from config import OUT

def build_dim_time() -> pd.DataFrame:
    """
    Builds a static time dimension table from 1800 to 2024.
    """
    years = range(1800, 2025)
    dim_time = pd.DataFrame({'year': years})
    dim_time['time_key'] = dim_time.index
    dim_time['decade'] = (dim_time['year'] // 10) * 10
    dim_time['five_year_period'] = (dim_time['year'] // 5) * 5
    dim_time['is_crisis_year'] = dim_time['year'].isin([2008, 2020])
    dim_time['is_pre_covid'] = dim_time['year'] < 2020
    dim_time['is_post_covid'] = dim_time['year'] >= 2020

    dim_time.to_csv(OUT["DIM_TIME"], index=False)
    print(f"[INFO] Dimension 'Dim_Time' built with {len(dim_time)} years.")
    return dim_time
