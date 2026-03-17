import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv()

def load_data(df):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )

        cursor = conn.cursor()
    except Exception as e:
        raise RuntimeError(f"Koneksi ke database gagal karena: {e}")

    ## Insert data ke database
    # Data outlet
    try:
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
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Data gagal di load ke database karena: {e}")   
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
