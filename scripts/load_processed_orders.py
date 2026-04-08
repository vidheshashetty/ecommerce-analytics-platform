import snowflake.connector

# ---- CONFIG ----
SNOWFLAKE_CONFIG = {
    user="Vidhesha",
    password="Vidhesha@190920",
    account="NRHDTCI-ND23225",
    warehouse="COMPUTE_WH",
    database="ECOMMERCE_DB",
    schema="BRONZE"
}

S3_PATH = "s3://ecommerce/processed/orders/"

def load_to_snowflake():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    try:
        print("Loading processed parquet data into Snowflake...")

        cursor.execute(f"""
        CREATE STAGE IF NOT EXISTS orders_2_stage
        URL = '{S3_PATH}'
        FILE_FORMAT = (TYPE = PARQUET)
        """)

        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders_2 (
            order_id INT,
            customer_id INT,
            product_id INT,
            order_date DATE,
            ship_date DATE,
            region STRING,
            country STRING,
            payment_method STRING,
            quantity INT,
            unit_price FLOAT,
            discount FLOAT,
            total_amount FLOAT,
            payment_status STRING,
            payment_timestamp TIMESTAMP
        )
        """)

        
        cursor.execute("""
        COPY INTO orders_large
        FROM @orders_stage
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)

        print("Data loaded into Snowflake Successfully")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_to_snowflake()
