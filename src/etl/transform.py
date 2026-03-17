import pandas as pd

# Function untuk mapping missing value menggunakan untuk data yang terlihat seperti key-value
def mapping(df, key, value):
    missing_value = df[key].isna().sum()
    print(f"{missing_value} missing value pada kolom '{key}'")

    if missing_value == 0:
        print(f"Tidak ada missing value pada kolom '{key}'")
        return df
    
    mapping = (
        df.dropna(subset=[key, value])
        .drop_duplicates(subset=[value]).
        set_index(value)[key]
        .to_dict()
    )

    df.loc[df[key].isna(), key] = df.loc[df[key].isna(), value].map(
        mapping
    )

    after_mapping_missing_value = df[key].isna().sum()
    print(f"{after_mapping_missing_value} missing value pada kolom '{key}' setelah mapping")

    if after_mapping_missing_value > 0:
        print(f"{after_mapping_missing_value} data gagal di-map pada {key}")

    return df

def transform_data(df):
    ## Error Handling dataset yang tidak ditemukan, dataset kosong, dan kolom yang tidak sesuai
    if df is None:
        raise ValueError("Dataset tidak ditemukan")
    if df.empty:
        raise ValueError("Dataset kosong")
    
    required_columns = {"outlet_code", "outlet_name", "product_code", "product_name", "product_price", "sales_period (DD/MM/YYYY)", "qty", "actual_sales"}
    actual_columns = set(df.columns)
    missing_columns = required_columns - actual_columns

    if missing_columns:
        raise ValueError("Kolom di dataset tidak sesuai")

    # logging jumlah data sebelum transformasi
    print("Jumlah data sebelum transformasi:", df.shape[0])

    ## Handling Missing Values
    # Mencari nilai key-value menggunakan mapping antara kolom 'outlet_code' dan 'outlet_name'
    df = mapping(df, "outlet_code", "outlet_name")

    # Mencari nilai key-value menggunakan mapping antara kolom  'product_code' dan 'product_name'
    df = mapping(df, "product_code", "product_name")

    """ Karena sudah tidak ada pasangan key-value lagi di database jadi kita lanjut mengurus missing value untuk data yang tidak memiliki pasangan key-value """
    # Menghapus baris yang memiliki missing value pada kolom 'outlet_code', 'product_code', dan 'product_name'
    print(df.shape[0], "data sebelum menghapus missing value pada kolom 'outlet_code', 'product_code', dan 'product_name'")
    
    df = df.dropna(subset=["product_code","product_name", "outlet_code" , "outlet_name"])
    
    print(df.shape[0], "data setelah menghapus missing value pada kolom 'outlet_code', 'product_code', dan 'product_name'")
   
    # Asumsi saya jika qty nya kosong dan actual_sales nya 0 maka data transaksi tersebut tidak valid dan tidak perlu dimasukkan ke dalam database, maka saya akan menghapus data tersebut
    print(df.shape[0], "data sebelum menghapus data yang memiliki missing value pada kolom 'qty' dan 'actual_sales' bernilai 0")
    
    df = df.drop(df[(df["qty"].isna()) & (df["actual_sales"] == 0)].index)
    
    print(df.shape[0], "data setelah menghapus data yang memiliki missing value pada kolom 'qty' dan 'actual_sales' bernilai 0")

    ## Handling Data Type
    # Ubah Data Type pada kolom 'sales_period (DD/MM/YYYY)' menjadi datetime
    df["sales_period (DD/MM/YYYY)"] = pd.to_datetime(
        df["sales_period (DD/MM/YYYY)"],
        format="%d/%m/%Y",
        errors="coerce"
    )

    # Rename kolom 'sales_period (DD/MM/YYYY)' menjadi 'sales_period'
    df = df.rename(columns={
        "sales_period (DD/MM/YYYY)" : "sales_period"
    })

    # Ubah Data Type pada kolom 'product_code' menjadi string
    df["product_code"] = (df["product_code"].astype("Int64").astype("string"))

    # Ubah Data Type pada kolom 'qty' menjadi integer, validasi sebelum diubah data yang kosong di hapus terlebih dahulu
    df = df.dropna(subset=["qty"])
    df["qty"] = df["qty"].astype("int64")

    # Validasi Data
    df = df[df["qty"] * df["product_price"] == df["actual_sales"]]
    df = df[df["qty"] >= 0]
    df = df[df["product_price"] >= 0]
    df = df[df["actual_sales"] >= 0]

    # logging jumlah data setelah transformasi
    print("Jumlah data setelah transformasi:", df.shape[0])

    return df
