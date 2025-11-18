import pandas as pd
from config import OUT

def build_dim_sector():
    """
    Builds the sector dimension table based on assumed ISIC categories.
    """
    sector_data = [
        # Primary Sector
        ('A', 'Agriculture, forestry and fishing', 'Primary'),
        ('B', 'Mining and quarrying', 'Primary'),

        # Secondary Sector
        ('C', 'Manufacturing', 'Secondary'),
        ('D', 'Electricity, gas, steam and air conditioning supply', 'Secondary'),
        ('E', 'Water supply; sewerage, waste management and remediation activities', 'Secondary'),
        ('F', 'Construction', 'Secondary'),

        # Tertiary Sector
        ('G', 'Wholesale and retail trade; repair of motor vehicles and motorcycles', 'Tertiary'),
        ('H', 'Transportation and storage', 'Tertiary'),
        ('I', 'Accommodation and food service activities', 'Tertiary'),
        ('J', 'Information and communication', 'Tertiary'),
        ('K', 'Financial and insurance activities', 'Tertiary'),
        ('L', 'Real estate activities', 'Tertiary'),
        ('M', 'Professional, scientific and technical activities', 'Tertiary'),
        ('N', 'Administrative and support service activities', 'Tertiary'),
        ('O', 'Public administration and defence; compulsory social security', 'Tertiary'),
        ('P', 'Education', 'Tertiary'),
        ('Q', 'Human health and social work activities', 'Tertiary'),
        ('R', 'Arts, entertainment and recreation', 'Tertiary'),
        ('S', 'Other service activities', 'Tertiary'),
        ('T', 'Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use', 'Tertiary'),
        ('U', 'Activities of extraterritorial organizations and bodies', 'Tertiary')
    ]

    dim_sector = pd.DataFrame(sector_data, columns=['isic_code', 'sector_name', 'sector_category'])
    dim_sector['sector_key'] = dim_sector.index
    
    dim_sector.to_csv(OUT["DIM_SECTOR"], index=False)
    print(f"✓ Dimension 'Dim_Sector' built with {len(dim_sector)} sectors.")
    return dim_sector
