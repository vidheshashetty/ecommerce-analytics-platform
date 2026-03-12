import snowflake.connector

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

    cur.execute(r"""
    COPY INTO bronze_customers
    FROM @ecommerce_stage
    FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1)
    PATTERN='.*customers_.*\\.csv\\.gz'
    ON_ERROR='CONTINUE'
    """)

    cur.execute(r"""
    COPY INTO bronze_orders
    FROM @ecommerce_stage
    FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1)
    PATTERN='.*orders_.*\\.csv\\.gz'
    ON_ERROR='CONTINUE'
    """)

    cur.execute(r"""
    COPY INTO bronze_payments
    FROM @ecommerce_stage
    FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1)
    PATTERN='.*payments_.*\\.csv\\.gz'
    ON_ERROR='CONTINUE'
    """)

    cur.execute(r"""
    COPY INTO bronze_events
    FROM @ecommerce_stage
    FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1)
    PATTERN='.*app_events_.*\\.csv\\.gz'
    ON_ERROR='CONTINUE'
    """)

    print("Data loaded successfully!")

finally:
    cur.close()
    conn.close()
