import pandas as pd
df = pd.read_csv('data/UNE_DEAP_SEX_AGE_RT_A-20251112T2214.csv')
print(df['classif1.label'].unique())
