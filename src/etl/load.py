from psycopg2.extras import execute_batch
from config import connection_db

# Function untuk insert data ke database
def insert_data(cursor, df, table_name, columns, conflict_columns):
    data = df[columns]

    data_table = [
        tuple(row[col] for col in columns)
        for _, row in data.iterrows()
    ]

    cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    query = f"""
        INSERT INTO {table_name} ({cols})
        VALUES({placeholders})
        ON CONFLICT ({", ".join(conflict_columns)}) DO NOTHING
    """

    execute_batch(cursor, query, data_table)

def load_data(df):
    conn, cursor = None, None
    ## Insert data ke database
    try:
        conn, cursor = connection_db()

        # Data outlet
        insert_data(cursor, df, "outlets", ["outlet_code", "outlet_name"], ["outlet_code"])

        # Data product
        insert_data(cursor, df, "products", ["product_code", "product_name"], ["product_code"])

        # Data outlet_product
        insert_data(cursor, df, "outlets_products", ["outlet_code", "product_code", "product_price"], ["outlet_code", "product_code"])
        
        # Data sales
        insert_data(cursor, df, "sales", ["sales_period", "outlet_code", "product_code", "qty", "actual_sales"], ["sales_period", "outlet_code", "product_code"])
        
        print("Data berhasil di load ke database")

        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise RuntimeError(f"Data gagal di load ke database karena: {e}")   
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
