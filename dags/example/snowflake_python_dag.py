import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 5),
}

# Define the DAG
dag = DAG(
    dag_id="snowflake_query_hook",
    default_args=default_args,
    schedule_interval=None,
    description='Run a simple query in Snowflake using SnowflakeHook',
    tags=['example'],
)

# Snowflake query
query = [
    """SELECT CURRENT_VERSION();"""
]

# Python function to run using SnowflakeHook
def run_query_with_hook(**context):
    dwh_hook = SnowflakeHook(
        snowflake_conn_id="snowflake_conn",
    )
    result = dwh_hook.get_first("SELECT CURRENT_VERSION()")
    logging.info("Snowflake Version: %s", result[0])


# Define tasks
with dag:
    # Task to execute the query using SnowflakeOperator
    query_exec = SnowflakeOperator(
        task_id="snowflake_operator_task",
        sql=query,
        snowflake_conn_id="snowflake_conn",
    )

    # Task to execute the query using SnowflakeHook in a Python function
    hook_query_task = PythonOperator(
        task_id="hook_query_task",
        python_callable=run_query_with_hook,
    )

# Define task dependencies
query_exec >> hook_query_task