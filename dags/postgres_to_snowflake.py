from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 30, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'postgres_to_snowflake',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
)

# Define PostgreSQL and Snowflake connections
postgres_conn_id = 'books_connection'
snowflake_conn_id = 'snowflake_connection'

# Task to extract data from PostgreSQL
extract_postgres_data = PostgresOperator(
    task_id='extract_postgres_data',
    postgres_conn_id=postgres_conn_id,
    sql='SELECT * FROM book_data;',
    dag=dag,
)

# Python function to process data (if needed)
def process_data(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_postgres_data')
    # Process the data here if needed
    return extracted_data

# Task to process data
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

# Task to load data into Snowflake
load_snowflake_data = SnowflakeOperator(
    task_id='load_snowflake_data',
    snowflake_conn_id=snowflake_conn_id,
    sql="""
    INSERT INTO your_snowflake_table (column1, column2, ...)
    VALUES {{ ti.xcom_pull(task_ids='process_data') }}
    """,
    dag=dag,
)

# Set task dependencies
extract_postgres_data >> process_data_task >> load_snowflake_data