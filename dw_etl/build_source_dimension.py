import pandas as pd
from config import OUT

def build_dim_source():
    """
    Builds the source dimension table.
    """
    source_data = [
        ('WIID', 'World Income Inequality Database', 'UNU-WIDER', 'High', 'Annual', 1890, 2023),
        ('ILO', 'International Labour Organization', 'ILO', 'High', 'Annual', 1946, 2024),
        ('WB', 'World Bank', 'World Bank Group', 'High', 'Annual', 1963, 2024),
        ('UNDP', 'United Nations Development Programme', 'UNDP', 'High', 'Annual', 2023, 2023),
        ('OWID', 'Our World in Data', 'Global Change Data Lab', 'Medium', 'Varies', 1800, 2023),
        ('PIT', 'Personal Income Tax Rates', 'Various (compiled)', 'Medium', 'Varies', 2023, 2023)
    ]

    dim_source = pd.DataFrame(source_data, columns=[
        'source_code', 'full_name', 'organization', 'data_quality_rating',
        'update_frequency', 'coverage_start_year', 'coverage_end_year'
    ])
    
    dim_source['source_key'] = dim_source.index

    dim_source.to_csv(OUT["DIM_SOURCE"], index=False)
    print(f"[INFO] Dimension 'Dim_Source' built with {len(dim_source)} sources.")
    return dim_source
