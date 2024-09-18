from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def use_http_connection(**kwargs):
    # Retrieve the connection object
    connection = BaseHook.get_connection('http_conn_albourne')

    # Extract the required attributes
    username = connection.login
    password = connection.password
    base_url = connection.host

    # Use these attributes in your code
    print(f"Username: {username}")
    print(f"Password: {password}")
    print(f"Base URL: {base_url}")


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'example_use_http_connection_dag',
    default_args=default_args,
    tags=["example"],
    schedule_interval='@daily',
)

use_connection_task = PythonOperator(
    task_id='use_http_connection',
    python_callable=use_http_connection,
    provide_context=True,
    dag=dag,
)