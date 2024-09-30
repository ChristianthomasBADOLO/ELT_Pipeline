from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.snowflake.transfers.postgres_to_snowflake import PostgresToSnowflakeOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 30, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'postgres_to_snowflake',
    default_args=default_args,
    schedule_interval='@daily',  # Set your desired schedule
)
# Define PostgreSQL and Snowflake connections
postgres_conn_id = 'your_postgres_connection'
snowflake_conn_id = 'your_snowflake_connection'

# Example task to extract data from PostgreSQL
extract_postgres_data = PostgresOperator(
    task_id='extract_postgres_data',
    postgres_conn_id=postgres_conn_id,
    sql='SELECT * FROM your_table;',
    dag=dag,
)

# Example task to load data into Snowflake
load_snowflake_data = PostgresToSnowflakeOperator(
    task_id='load_snowflake_data',
    postgres_conn_id=postgres_conn_id,
    snowflake_conn_id=snowflake_conn_id,
    sql='SELECT * FROM your_table WHERE ds = {{ ds }};',
    snowflake_table='your_snowflake_table',
    dag=dag,
)

# Set task dependencies
extract_postgres_data >> load_snowflake_data