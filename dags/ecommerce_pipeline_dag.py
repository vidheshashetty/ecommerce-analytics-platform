from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'vidhesha',
    'start_date': datetime(2026, 3, 3),
    'retries': 1,
    'end_date': datetime(2026, 6, 6),
    'retry_delay': timedelta(minutes=1),
    'email': ['vidheshashetty@gmail.com'],
    'email_on_retry':False,
    'email_on_failure':True
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args = default_args,
    schedule="@daily",
    catchup=False
) as dag:

    generate_data = BashOperator(
    task_id="generate_incremental_data",
    bash_command="python /root/projects/ecommerce-analytics-platform/scripts/ecommerce_incremental_data.py"
    )

    load_raw_data = BashOperator(
    task_id="load_raw_data_to_snowflake",
    bash_command="""python /root/projects/ecommerce-analytics-platform/scripts/upload_to_stage.py &&
    python /root/projects/ecommerce-analytics-platform/scripts/load_to_snowflake.py
    """
    )

    run_dbt_staging = BashOperator(
    task_id="run_dbt_staging",
    bash_command="""
    source /root/projects/ecommerce-analytics-platform/dbt_env/bin/activate &&
    cd /root/projects/ecommerce-analytics-platform/ecommerce_dbt && dbt run --select staging
    """)

    run_dbt_marts = BashOperator(
    task_id="run_dbt_marts",
    bash_command="""
    source /root/projects/ecommerce-analytics-platform/dbt_env/bin/activate &&
    cd /root/projects/ecommerce-analytics-platform/ecommerce_dbt && dbt run --select marts
    """
    )

    run_dbt_tests = BashOperator(
    task_id="run_dbt_tests",
    bash_command="""
    source /root/projects/ecommerce-analytics-platform/dbt_env/bin/activate &&
    cd /root/projects/ecommerce-analytics-platform/ecommerce_dbt && dbt test
    """
    )

generate_data >> load_raw_data >> run_dbt_staging >> run_dbt_marts >> run_dbt_tests
