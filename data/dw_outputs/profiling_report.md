# Data Profiling Report (Step 1)

### WIID Country (wiidcountry_4.xlsx)
- rows: 2,797
- years: 1890–2023 (distinct=85)
- top null ratios: former=83.0%, y95=9.0%, y39=9.0%, y27=9.0%, y28=9.0%

### STAGING_WIID_Inequality
- rows: 2,786
- years: 1890–2023 (distinct=85)
- countries: 202
- top null ratios: gini=8.9%, palma=8.9%, s80s20=8.9%, country_name=0.0%, iso3=0.0%

### ILO raw staging: EMP_NIFL_SEX_RT_A
- rows: 3,840
- years: 1999–2024 (distinct=26)
- countries: 144
- top null ratios: note_indicator=89.5%, obs_status=89.5%, value=0.1%, country_name=0.0%, source=0.0%

### ILO raw staging: UNE_DEAP_SEX_AGE_RT_A
- rows: 282,464
- years: 1947–2024 (distinct=78)
- countries: 225
- top null ratios: note_classif=97.6%, note_indicator=85.9%, obs_status=85.5%, note_source=5.8%, value=4.2%

### ILO raw staging: EMP_DWAP_SEX_AGE_RT_A
- rows: 254,137
- years: 1947–2024 (distinct=78)
- countries: 222
- top null ratios: note_classif=99.0%, obs_status=92.3%, note_indicator=92.2%, note_source=3.9%, value=0.1%

### ILO raw staging: EAR_4MTH_SEX_CUR_NB_A
- rows: 21,637
- years: 1969–2024 (distinct=56)
- countries: 189
- top null ratios: obs_status=93.6%, note_classif=90.9%, note_source=3.3%, country_name=0.0%, source=0.0%

### Dim_Sex
- rows: 4
- top null ratios: sex_key=0.0%, sex=0.0%

### Dim_Age
- rows: 32
- top null ratios: age_key=0.0%, age_group=0.0%

### Dim_Sector
- rows: 3
- top null ratios: sector_key=0.0%, sector=0.0%

### Dim_Temps
- rows: 135
- years: 1890–2024 (distinct=135)
- top null ratios: year=0.0%, decade=0.0%

### Dim_Indicateur_seed
- rows: 8
- top null ratios: indicator_name=0.0%, unit=0.0%, source=0.0%, indicator_code=0.0%

