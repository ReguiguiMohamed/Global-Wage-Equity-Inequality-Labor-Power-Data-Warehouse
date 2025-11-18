import pandas as pd
df = pd.read_csv('data/EAR_4MTH_SEX_ECO_CUR_NB_A-20251116T2211.csv')
print(df['classif1.label'].unique())
