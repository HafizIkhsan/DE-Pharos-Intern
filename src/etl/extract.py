import pandas as pd

def extract_data():
    url = "https://docs.google.com/spreadsheets/d/1WE17277HEMHrfa7IU6TlVhbzVrFqWNBZQ4qDfAvImUI/export?format=csv"
    try:
        df = pd.read_csv(url)

        print("Data berhasil di load:")
        print(df.shape[0], "baris dan", df.shape[1], "kolom")

        return df
    except Exception as e:
        raise RuntimeError(f"Data gagal di ambil karena: {e}")