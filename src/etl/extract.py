import pandas as pd
import os

def extract_data():
    url = os.getenv("dataset_url")
    try:
        df = pd.read_csv(url)

        print("Data berhasil di load:")
        print(df.shape[0], "baris dan", df.shape[1], "kolom")

        return df
    except Exception as e:
        raise RuntimeError(f"Data gagal di ambil karena: {e}")