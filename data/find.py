import pandas as pd
from pathlib import Path

# --- CONFIGURATION ---
CONFIG = {
    # REQUIRED: Source file for ALL World Bank region names
    "WIID_REGION_MAP_XLSX": "wiidglobal_2.xlsx", 
    
    # REQUIRED: The resulting aggregated file
    "ETD_REGION_OUTPUT_CSV": "dw_regional_etd_output_v2/Fact_PolicyEconomy_RegionWB.csv",
}

# -----------------------------
# 1. LOADER FUNCTION
# -----------------------------

def load_wiid_regions(path: Path) -> set:
    """
    Loads all distinct World Bank region names from the WIID Excel file.
    """
    if not path.exists():
        print(f"Error: WIID region map file not found at {path}")
        return set()
    
    try:
        xls = pd.ExcelFile(path)
        dfs = [pd.read_excel(path, sheet_name=s) for s in xls.sheet_names]
        wiid = pd.concat(dfs, ignore_index=True)
        
        # Standardize column names and extract the region column
        wiid.columns = [str(c).strip().lower() for c in wiid.columns]
        
        if 'region_wb' not in wiid.columns:
            print("Error: 'region_wb' column not found in WIID file.")
            return set()
            
        # Extract all unique, non-missing region names
        regions = set(wiid['region_wb'].dropna().unique())
        return regions
        
    except Exception as e:
        print(f"An error occurred while loading WIID regions: {e}")
        return set()

# -----------------------------
# 2. ANALYSIS FUNCTION
# -----------------------------

def identify_missing_regions():
    """
    Compares the full list of WB regions to the regions found in the output.
    """
    # 1. Get the Universe of ALL WB Regions
    wiid_path = Path(CONFIG["WIID_REGION_MAP_XLSX"])
    all_wb_regions = load_wiid_regions(wiid_path)
    
    if not all_wb_regions:
        print("\nCannot proceed: Failed to load the full list of WB regions.")
        return

    # 2. Get Regions Present in the Final Output
    output_path = Path(CONFIG["ETD_REGION_OUTPUT_CSV"])
    if not output_path.exists():
        print(f"\nError: Output CSV not found at {output_path}. Check the path/name.")
        return
        
    try:
        output_df = pd.read_csv(output_path)
        
        # The output column is 'region_wb_label' as per your original code
        if 'region_wb_label' not in output_df.columns:
            print("Error: 'region_wb_label' column not found in the output CSV.")
            return

        present_regions = set(output_df['region_wb_label'].dropna().unique())

    except Exception as e:
        print(f"An error occurred while loading the output file: {e}")
        return
        
    # 3. Calculate the Difference
    missing_regions = all_wb_regions - present_regions
    
    # 4. Display Results
    print(f"\n--- WB Region Discrepancy Analysis ---")
    print(f"Total Regions Expected (from {wiid_path.name}): {len(all_wb_regions)}")
    print(f"Total Regions Found (in Output CSV): {len(present_regions)}")
    print("-" * 38)
    
    if missing_regions:
        print(f"❌ **{len(missing_regions)} Regions Are Missing in the Aggregation:**")
        for region in sorted(missing_regions):
            print(f"  - {region}")
    else:
        print("✅ All expected regions are present in the aggregated output.")

# -----------------------------
# MAIN EXECUTION
# -----------------------------
if __name__ == "__main__":
    identify_missing_regions()