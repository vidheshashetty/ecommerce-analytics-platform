import os
import snowflake.connector

INCREMENTAL_PATH = "/root/projects/ecommerce-analytics-platform/data/raw_data/incremental/"

conn = snowflake.connector.connect(
    user="Vidhesha",
    password="Vidhesha@190920",
    account="NRHDTCI-ND23225",
    warehouse="COMPUTE_WH",
    database="ECOMMERCE_DB",
    schema="BRONZE"
)

cur = conn.cursor()

try:
    for file_name in os.listdir(INCREMENTAL_PATH):
        if file_name.endswith(".csv"):
            file_path = os.path.join(INCREMENTAL_PATH, file_name)
            print(f"Uploading {file_name} to stage...")
            cur.execute(f"""
            PUT file://{file_path} @ecommerce_stage AUTO_COMPRESS=TRUE;
            """)    


finally:
    cur.close()
    conn.close()
