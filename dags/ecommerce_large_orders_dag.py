from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'vidhesha',
    'start_date': datetime(2026, 3, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email': ['vidheshashetty@gmail.com'],
    'email_on_retry': False,
    'email_on_failure': True
}

with DAG(
    dag_id="ecommerce_large_orders_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False
) as dag:

    upload_large_orders = BashOperator(
        task_id="upload_large_orders_to_s3",
        bash_command="python /root/projects/ecommerce-analytics-platform/scripts/upload_large_orders_to_s3.py"
    )

    
    run_glue_job = BashOperator(
        task_id="run_glue_pyspark_job",
        bash_command="""
        aws glue start-job-run \
        --job-name ecommerce-large-orders-job
        """
    )

    
    load_processed_data = BashOperator(
        task_id="load_processed_orders_to_snowflake",
        bash_command="python /root/projects/ecommerce-analytics-platform/scripts/load_processed_orders.py"
    )

    
    run_dbt_large_models = BashOperator(
        task_id="run_dbt_large_models",
        bash_command="""
        source /root/projects/ecommerce-analytics-platform/dbt_env/bin/activate &&
        cd /root/projects/ecommerce-analytics-platform/ecommerce_dbt &&
        dbt run --select fact_orders_large
        """
    )

    
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests_large",
        bash_command="""
        source /root/projects/ecommerce-analytics-platform/dbt_env/bin/activate &&
        cd /root/projects/ecommerce-analytics-platform/ecommerce_dbt &&
        dbt test
        """
    )

upload_large_orders >> run_glue_job >> load_processed_data >> run_dbt_large_models >> run_dbt_tests
