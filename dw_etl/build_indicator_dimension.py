import pandas as pd
from config import OUT

def build_dim_indicator():
    """
    Builds the indicator dimension table.
    """
    indicator_data = [
        # ILOSTAT
        ('unemployment_rate', 'Unemployment Rate', 'Economy', 'Labor Market', 'Unemployment', '%', 'ILO'),
        ('employment_to_population_ratio', 'Employment to Population Ratio', 'Economy', 'Labor Market', 'Employment', '%', 'ILO'),
        ('labour_force_participation_rate', 'Labour Force Participation Rate', 'Economy', 'Labor Market', 'Participation', '%', 'ILO'),
        ('informal_employment_rate', 'Informal Employment Rate', 'Economy', 'Labor Market', 'Informality', '%', 'ILO'),
        ('youth_neet_rate', 'Youth NEET Rate', 'Social', 'Youth', 'NEET', '%', 'ILO'),
        ('avg_monthly_earnings', 'Average Monthly Earnings', 'Economy', 'Wages', 'Earnings', 'USD', 'ILO'),
        # Minimum wage is treated as part of social development
        ('minimum_wage', 'Minimum Wage', 'Social', 'Wages', 'Minimum Wage', 'USD', 'ILO'),

        # World Bank
        ('literacy_rate', 'Literacy Rate', 'Social', 'Education', 'Literacy', '%', 'WB'),
        ('gini_wb', 'Gini Index (World Bank)', 'Inequality', 'Income Inequality', 'Gini', 'index', 'WB'),

        # WIID
        ('gini', 'Gini Index (WIID)', 'Inequality', 'Income Inequality', 'Gini', 'index', 'WIID'),
        ('palma', 'Palma Ratio', 'Inequality', 'Income Inequality', 'Ratio', 'ratio', 'WIID'),
        ('s80s20', 'S80/S20 Ratio', 'Inequality', 'Income Inequality', 'Ratio', 'ratio', 'WIID'),
        ('gini_std', 'Gini Index (Standardized)', 'Inequality', 'Income Inequality', 'Gini', 'index', 'WIID'),

        # UNDP
        ('hdi', 'Human Development Index', 'Social', 'Development', 'HDI', 'index', 'UNDP'),

        # OWID
        ('top_10_percent_share', 'Top 10% Income Share', 'Inequality', 'Income Inequality', 'Share', '%', 'OWID'),
        ('top_1_percent_share', 'Top 1% Income Share', 'Inequality', 'Income Inequality', 'Share', '%', 'OWID'),
        ('health_expenditure_per_capita', 'Health Expenditure per Capita', 'Social', 'Health', 'Expenditure', 'USD', 'OWID'),
        ('inequality_education', 'Inequality in Education', 'Inequality', 'Education Inequality', 'Inequality', 'index', 'OWID'),
        ('inequality_life_expectancy', 'Inequality in Life Expectancy', 'Inequality', 'Health Inequality', 'Inequality', 'index', 'OWID'),
        # Government spending is also merged into social development
        ('gov_spending_gdp_percent', 'Government Spending as % of GDP', 'Social', 'Fiscal Policy', 'Spending', '%', 'OWID'),
        ('cv_caloric_intake', 'Coefficient of Variation of Caloric Intake', 'Social', 'Nutrition', 'Inequality', 'index', 'OWID'),
    ]

    dim_indicator = pd.DataFrame(indicator_data, columns=[
        'indicator_code', 'indicator_name', 'domain', 'theme', 'category', 'unit', 'source'
    ])
    
    dim_indicator['indicator_key'] = dim_indicator.index

    dim_indicator.to_csv(OUT["DIM_INDICATOR"], index=False)
    print(f"[INFO] Dimension 'Dim_Indicator' built with {len(dim_indicator)} indicators.")
    return dim_indicator
