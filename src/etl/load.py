import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv()

def load_data(df):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

    cursor = conn.cursor()

    ## Insert data ke database
    # Data outlet
    outlets = df[["outlet_code", "outlet_name"]].drop_duplicates()

    data_outlet = [
        (row["outlet_code"], row["outlet_name"])
        for _, row in outlets.iterrows()
    ]

    execute_batch(cursor, """
        INSERT INTO outlets (outlet_code, outlet_name)
        VALUES (%s, %s)
        ON CONFLICT (outlet_code) DO NOTHING
        """, data_outlet)

    # Data product
    products = df[["product_code", "product_name"]].drop_duplicates()

    data_product = [
        (row["product_code"], row["product_name"])
        for _, row in products.iterrows()
    ]

    execute_batch(cursor, """
        INSERT INTO products (product_code, product_name)
        VALUES (%s, %s)
        ON CONFLICT (product_code) DO NOTHING
        """, data_product)

    # Data outlet_product
    outlet_product = df[["outlet_code", "product_code", "product_price"]].drop_duplicates()

    data_outlet_product = [
        (row["outlet_code"], row["product_code"], row["product_price"])
        for _, row in outlet_product.iterrows()
    ]

    execute_batch(cursor, """
        INSERT INTO outlets_products (outlet_code, product_code, product_price)
        VALUES (%s, %s, %s)
        ON CONFLICT (outlet_code, product_code) DO NOTHING
        """, data_outlet_product)
    
    # Data sales
    sales = df[["sales_period", "outlet_code", "product_code", "qty", "actual_sales"]].drop_duplicates()

    data_sales = [
        (row["sales_period"], row["outlet_code"], row["product_code"], row["qty"], row["actual_sales"])
        for _, row in sales.iterrows()
    ]

    execute_batch(cursor, """
        INSERT INTO sales (sales_period, outlet_code, product_code, qty, actual_sales)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (sales_period, outlet_code, product_code) DO NOTHING
        """, data_sales)

    conn.commit()
    cursor.close()
    conn.close()
