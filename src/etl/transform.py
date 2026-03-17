import pandas as pd

def transform_data(df):
    # logging jumlah data sebelum transformasi
    print("Jumlah data sebelum transformasi:", df.shape[0])

    # Handling Missing Values
    # Mencari nilai key-value menggunakan mapping antara kolom 'outlet_code' dan 'outlet_name'
    mapping_outlet = df.dropna(subset=["outlet_code", "outlet_name"]).drop_duplicates(subset=["outlet_name"]).set_index("outlet_name")["outlet_code"].to_dict()

    df.loc[df["outlet_code"].isna(), "outlet_code"] = df.loc[df["outlet_code"].isna(), "outlet_name"].map(
        mapping_outlet
    )

    # Mencari nilai key-value menggunakan mapping antara kolom  'product_code' dan 'product_name'
    mapping_product = df.dropna(subset=["product_code", "product_name"]).drop_duplicates(subset=["product_name"]).set_index("product_name")["product_code"].to_dict()

    df.loc[df["product_code"].isna(), "product_code"] = df.loc[df["product_code"].isna(), "product_name"].map(
        mapping_product
    )

    """ Karena sudah tidak ada pasangan key-value lagi di database jadi kita lanjut mengurus missing value untuk data yang tidak memiliki pasangan key-value """
    # Menghapus baris yang memiliki missing value pada kolom 'outlet_code', 'product_code', dan 'product_name'
    df = df.dropna(subset=["product_code","product_name", "outlet_code" , "outlet_name"])

    # Asumsi saya jika qty nya kosong dan actual_sales nya 0 maka data transaksi tersebut tidak valid dan tidak perlu dimasukkan ke dalam database, maka saya akan menghapus data tersebut
    df = df.drop(df[(df["qty"].isna()) & (df["actual_sales"] == 0)].index)

    # Handling Data Type
    # Ubah Data Type pada kolom 'sales_period (DD/MM/YYYY)' menjadi datetime
    df["sales_period (DD/MM/YYYY)"] = pd.to_datetime(
        df["sales_period (DD/MM/YYYY)"],
        format="%d/%m/%Y"
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
