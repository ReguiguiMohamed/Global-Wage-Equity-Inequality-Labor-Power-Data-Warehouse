import pandas as pd
from config import DATA_DIR, OUT, EXCLUDE_ISO3

def build_dim_geography():
    """Builds the conformed geography dimension from the WIID Global CSV file."""
    wiid_path = DATA_DIR / 'wiidglobal_2.csv'
    if not wiid_path.exists():
        print(f"Error: WIID global CSV file not found at {wiid_path}")
        return pd.DataFrame()

    wiid = pd.read_csv(wiid_path)
    wiid.columns = [str(c).strip().lower() for c in wiid.columns]

    # Select latest country info
    latest = wiid.sort_values(["country", "year"], ascending=[True, False]).groupby("country", as_index=False).first()

    # Rename columns and select required fields
    dim = latest.rename(columns={
        "country": "country_name",
        "c3": "iso3",
        "subarea": "subregion_un",
        "area": "region_un",
        "incomegroup": "income_group",
        "population": "population_latest",
        "gdp": "gdp_total"
    })

    required_cols = [
        "country_name", "iso3", "subregion_un", "region_un", 
        "income_group", "population_latest", "gdp_total"
    ]
    dim = dim[required_cols].copy()

    # Calculate GDP per capita
    dim['gdp_per_capita'] = dim['gdp_total'] / dim['population_latest']

    # Derive continent
    def get_continent(region):
        if region in ['Sub-Saharan Africa', 'Northern Africa']:
            return 'Africa'
        elif region in ['Latin America & Caribbean', 'Northern America']:
            return 'Americas'
        elif region in ['Western Asia', 'Central Asia', 'Eastern Asia', 'South-eastern Asia', 'Southern Asia']:
            return 'Asia'
        elif region in ['Western Europe', 'Eastern Europe', 'Southern Europe', 'Northern Europe']:
            return 'Europe'
        elif region in ['Australia & New Zealand', 'Melanesia', 'Micronesia', 'Polynesia']:
            return 'Oceania'
        else:
            return 'Other'
    
    dim['continent'] = dim['region_un'].apply(get_continent)

    # Add regional bloc flags
    oecd_countries = ['AUS', 'AUT', 'BEL', 'CAN', 'CHL', 'COL', 'CRI', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'ISL', 'IRL', 'ISR', 'ITA', 'JPN', 'KOR', 'LVA', 'LTU', 'LUX', 'MEX', 'NLD', 'NZL', 'NOR', 'POL', 'PRT', 'SVK', 'SVN', 'ESP', 'SWE', 'CHE', 'TUR', 'GBR', 'USA']
    eu_countries = ['AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU', 'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT', 'ROU', 'SVK', 'SVN', 'ESP', 'SWE']
    g20_countries = ['ARG', 'AUS', 'BRA', 'CAN', 'CHN', 'FRA', 'DEU', 'IND', 'IDN', 'ITA', 'JPN', 'MEX', 'RUS', 'SAU', 'ZAF', 'KOR', 'TUR', 'GBR', 'USA']

    dim['is_oecd'] = dim['iso3'].isin(oecd_countries)
    dim['is_eu'] = dim['iso3'].isin(eu_countries)
    dim['is_g20'] = dim['iso3'].isin(g20_countries)

    # Apply exclusions and cleaning
    dim = dim[~dim["iso3"].isin(EXCLUDE_ISO3)]
    dim = dim.dropna(subset=["iso3"]).drop_duplicates("iso3").sort_values("country_name").reset_index(drop=True)
    dim['geography_key'] = dim.index
    
    # Select final columns
    final_cols = [
        'geography_key', 'iso3', 'country_name', 'subregion_un', 'region_un', 'continent', 
        'income_group', 'population_latest', 'gdp_per_capita',
        'is_oecd', 'is_eu', 'is_g20'
    ]
    dim = dim[final_cols]

    dim.to_csv(OUT["DIM_GEOGRAPHY"], index=False)
    print(f"[INFO] Dimension 'Dim_Geography' built with {len(dim)} countries.")
    return dim