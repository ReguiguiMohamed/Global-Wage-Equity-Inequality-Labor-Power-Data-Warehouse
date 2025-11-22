import pandas as pd
import numpy as np

def clean_row_values(row):
    """
    Function to clean row values by handling potential NaN/inf values for numeric columns
    This is the same function added to the DAG
    """
    cleaned_row = []
    for val in row:
        # Check if the value is NaN, infinity, or other invalid numeric values
        if pd.isna(val) or (isinstance(val, float) and (val != val or val == float('inf') or val == float('-inf'))):
            cleaned_row.append(None)  # Convert to None which becomes NULL in SQL
        elif isinstance(val, str) and val.strip() == '':  # Empty string
            cleaned_row.append(None)
        elif val == '' or val == '#N/A' or val == 'NULL' or val == 'null' or val == 'nan' or val == 'NaN' or val == 'N/A' or val == 'NA':
            # Additional invalid values that should be converted to NULL
            cleaned_row.append(None)
        else:
            cleaned_row.append(val)
    return tuple(cleaned_row)

# Test with sample data that could cause the issue
test_data = [
    [9.0, 211, 0.0, 0.0, 2.0, 0, 8.9],  # Normal data
    [9.0, 211, 1.0, 0.0, 2.0, 0, np.nan],  # NaN value
    [9.0, 211, 2.0, 0.0, 2.0, 0, float('inf')],  # Positive infinity
    [9.0, 211, 0.0, 0.0, 2.0, 0, float('-inf')],  # Negative infinity
    [9.0, 211, 1.0, 0.0, 2.0, 0, ''],  # Empty string
    [9.0, 211, 2.0, 0.0, 2.0, 0, None],  # None value
    [9.0, 211, 1.0, 0.0, 2.0, 0, '#N/A'],  # #N/A string
    [9.0, 211, 2.0, 0.0, 2.0, 0, 'NULL'],  # 'NULL' string
    [9.0, 211, 1.0, 0.0, 2.0, 0, 'null'],  # 'null' string
    [9.0, 211, 2.0, 0.0, 2.0, 0, 'nan'],  # 'nan' string
    [9.0, 211, 1.0, 0.0, 2.0, 0, 'NaN'],  # 'NaN' string
    [9.0, 211, 2.0, 0.0, 2.0, 0, 'N/A'],  # 'N/A' string
    [9.0, 211, 1.0, 0.0, 2.0, 0, 'NA'],  # 'NA' string
    [9.0, 211, 2.0, 0.0, 2.0, 0, '   '],  # Whitespace-only string
]

print("Testing the clean_row_values function:")
print("Original data:", test_data)
print()

cleaned_data = [clean_row_values(row) for row in test_data]
print("Cleaned data:", cleaned_data)
print()

# Verify the function works as expected
for i, (original, cleaned) in enumerate(zip(test_data, cleaned_data)):
    print(f"Row {i+1}:")
    print(f"  Original: {original}")
    print(f"  Cleaned:  {cleaned}")
    # Check that the 'value' column (index 6) is properly handled
    original_value = original[6]
    cleaned_value = cleaned[6]

    # Check if the original value is one that should be converted to None
    should_be_none = (
        pd.isna(original_value) or
        original_value == float('inf') or
        original_value == float('-inf') or
        (isinstance(original_value, str) and original_value.strip() == '') or
        original_value in ['', '#N/A', 'NULL', 'null', 'nan', 'NaN', 'N/A', 'NA']
    )

    if should_be_none:
        assert cleaned_value is None, f"Row {i+1}: Expected None for invalid value {original_value}, got {cleaned_value}"
        print(f"  Status: OK - Invalid value {original_value} converted to None")
    else:
        assert cleaned_value == original_value, f"Row {i+1}: Value changed unexpectedly from {original_value} to {cleaned_value}"
        print(f"  Status: OK - Good value unchanged")
    print()

print("All tests passed! The fix should handle invalid float values correctly.")