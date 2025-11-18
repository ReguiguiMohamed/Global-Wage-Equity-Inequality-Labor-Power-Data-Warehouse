import pandas as pd

def get_unique_values_chunked(file_path, column_name):
    unique_values = set()
    chunk_size = 100000  # Adjust chunk size based on memory constraints
    try:
        for chunk in pd.read_csv(file_path, chunksize=chunk_size, on_bad_lines='skip'):
            unique_values.update(chunk[column_name].unique())
        return list(unique_values)
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == '__main__':
    file_path = 'data/EAR_4MTH_SEX_ECO_CUR_NB_A-20251116T2211.csv'
    column_name = 'classif1.label'
    unique_classif1 = get_unique_values_chunked(file_path, column_name)
    if unique_classif1:
        print(unique_classif1)
