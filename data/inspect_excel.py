import pandas as pd

xls = pd.ExcelFile('data/wiidglobal_2.xlsx')
for sheet in xls.sheet_names:
    print(f"Sheet: {sheet}")
    df = pd.read_excel(xls, sheet)
    print(f"Columns: {df.columns.tolist()}")
