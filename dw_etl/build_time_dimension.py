import pandas as pd
from config import OUT

def build_dim_time(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Builds a time dimension table from a list of dataframes.

    Args:
        dataframes: A list of pandas DataFrames, each with a 'year' column.

    Returns:
        A DataFrame representing the time dimension.
    """
    min_year, max_year = 9999, 0
    for df in dataframes:
        if 'year' in df.columns:
            min_year = min(min_year, df['year'].min())
            max_year = max(max_year, df['year'].max())

    years = range(int(min_year), int(max_year) + 1)
    dim_time = pd.DataFrame({'year': years})
    dim_time['time_key'] = dim_time.index
    dim_time['decade'] = (dim_time['year'] // 10) * 10
    dim_time['pre_2008_crisis_flag'] = dim_time['year'] < 2008
    dim_time['post_covid_flag'] = dim_time['year'] >= 2020

    dim_time.to_csv(OUT["DIM_TIME"], index=False)
    print(f"✓ Dimension 'Dim_Time' built with {len(dim_time)} years.")
    return dim_time
