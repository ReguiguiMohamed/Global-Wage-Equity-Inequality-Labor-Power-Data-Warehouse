import pandas as pd
from config import OUT

def build_dim_economic_classification(dim_geography: pd.DataFrame):
    """
    Builds the economic classification dimension table.
    """
    income_groups = dim_geography['income_group'].unique()
    
    def get_development_status(income_group):
        if income_group == 'High income':
            return 'Developed'
        elif income_group == 'Low income':
            return 'LDC'
        else:
            return 'Developing'

    classification_data = []
    for ig in income_groups:
        if pd.notna(ig):
            classification_data.append({
                'income_group': ig,
                'development_status': get_development_status(ig)
            })

    dim_economic_classification = pd.DataFrame(classification_data)
    dim_economic_classification['economic_classification_key'] = dim_economic_classification.index
    
    # Add temporal validity columns (with dummy values for now)
    dim_economic_classification['valid_from_year'] = 1800
    dim_economic_classification['valid_to_year'] = 2024

    dim_economic_classification.to_csv(OUT["DIM_ECONOMIC_CLASSIFICATION"], index=False)
    print(f"[INFO] Dimension 'Dim_Economic_Classification' built with {len(dim_economic_classification)} classifications.")
    return dim_economic_classification
