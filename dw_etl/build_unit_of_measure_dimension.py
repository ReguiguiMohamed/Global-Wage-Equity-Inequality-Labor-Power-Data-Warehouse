import pandas as pd
from config import OUT

def build_dim_unit_of_measure():
    """
    Builds the unit of measure dimension table.
    """
    unit_data = [
        ('%', 'Percentage', '0-100', 'Average'),
        ('ratio', 'Ratio', 'Varies', 'Average'),
        ('USD', 'US Dollars', 'Varies', 'Sum'),
        ('index', 'Index', 'Varies', 'Average')
    ]

    dim_unit = pd.DataFrame(unit_data, columns=[
        'unit_code', 'unit_name', 'unit_scale', 'aggregation_type'
    ])
    
    dim_unit['unit_key'] = dim_unit.index

    dim_unit.to_csv(OUT["DIM_UNIT_OF_MEASURE"], index=False)
    print(f"✓ Dimension 'Dim_Unit_of_Measure' built with {len(dim_unit)} units.")
    return dim_unit
