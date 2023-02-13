from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG object
dag = DAG(
    'ingest_transactions_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define a task to extract the data from the local database
extract_data = MySqlOperator(
    task_id='extract_data',
    sql='''
        SELECT timestamp, product_id, product_name, product_price, client_id
        FROM transactions
        WHERE timestamp BETWEEN '{{ yesterday_ds }} 00:00:00' AND '{{ yesterday_ds }} 23:59:59';
    ''',
    mysql_conn_id='mysql_conn',
    dag=dag
)


# Define a task to transform the extracted data
def transform_data(**kwargs):
    # Write your transformation logic here
    pass


transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)


# Define a task to load the transformed data to the data lake
def load_data_to_datalake(**kwargs):
    # Write your code to load the data to the data lake here
    pass


load_data_to_datalake = PythonOperator(
    task_id='load_data_to_datalake',
    python_callable=load_data_to_datalake,
    dag=dag
)

# Set up the task dependencies
extract_data >> transform_data >> load_data_to_datalake
