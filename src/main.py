from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data

if __name__ == "__main__":
    df = extract_data()
    clean_df = transform_data(df)
    load_data(clean_df)