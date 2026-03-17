import psycopg2
import os

def connection_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )

        cursor = conn.cursor()

        return conn, cursor
    except Exception as e:
        raise RuntimeError(f"Koneksi ke database gagal karena: {e}")