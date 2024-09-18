"""DAG that retrieves all funds from albourne and loads it into snowflake ."""

# --------------- #
# Package imports #
# --------------- #

import pandas as pd
from airflow.decorators import dag, task
from pendulum import datetime
from snowflake.connector.pandas_tools import write_pandas

# -------------------- #
# Local module imports #
# -------------------- #
from include.albourne_utils import (
    use_http_connection,
    get_bearer_token,
    get_all_funds,
)
from include.global_variables import airflow_conf_variables as gv
from include.global_variables import albourne_constants as ac


# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "DS_START" Dataset has been produced to
    schedule=None,
    catchup=False,
    default_args=gv.default_args,
    description="DAG that retrieves all funds and saves it to a local JSON.",
    tags=["albourne"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def extract_albourne_all_funds():
    @task
    def get_conn_info():
        _conn_info = use_http_connection()
        return _conn_info

    conn_info = get_conn_info()

    @task
    def get_token(conn_info):
        _username = conn_info["username"]
        _password = conn_info["password"]
        _token = get_bearer_token(_username, _password)
        return _token

    # token = get_token(av.USERNAME, av.PASSWORD)
    token = get_token(conn_info)

    @task
    def get_data(token):
        df = get_all_funds(token)
        return df

    all_funds = get_data(token)

    @task
    def mask_dataframe(df: pd.DataFrame, columns_to_mask: list):
        """
        Apply masking or anonymization to specific columns in the DataFrame.
        """
        masked_df = df.copy()

        for column in columns_to_mask:
            # Apply masking: you can replace values with 'XXX' or a hash, etc.
            masked_df[column] = masked_df[column].apply(lambda x: 'XXX' if isinstance(x, str) else x)

            # Alternatively, you can hash the data if you want to obfuscate it instead of simple masking
            # masked_df[column] = masked_df[column].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notnull(x) else x)

        return masked_df

    all_funds_mask = mask_dataframe(all_funds, ['fundName', 'managerName'])

    @task
    def turn_json_into_table(
            snowflake_db_conn_id: str, all_funds_table_name: str, all_funds: pd.DataFrame
    ):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        # Convert DataFrame column names to uppercase
        all_funds.columns = all_funds.columns.str.upper()

        all_funds_df = all_funds

        snowflake_db_conn = SnowflakeHook(snowflake_db_conn_id).get_conn()
        cursor = snowflake_db_conn.cursor()

        # Create table if it doesn't exist
        columns = ", ".join([f"{col} STRING" for col in all_funds.columns])  # Adjust data types if necessary
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {all_funds_table_name} ({columns})"
        cursor.execute(create_table_sql)

        truncate_table_sql = f"TRUNCATE TABLE {all_funds_table_name}"
        cursor.execute(truncate_table_sql)

        # Use the write_pandas function to upload the DataFrame to Snowflake
        success, nchunks, nrows, _ = write_pandas(snowflake_db_conn, all_funds, all_funds_table_name)

        # Check if the operation was successful
        if success:
            print(f"Successfully inserted {nrows} rows into {all_funds_table_name}")
        else:
            raise ValueError("Failed to insert data into Snowflake")

        cursor.close()

    turn_json_into_table(
        snowflake_db_conn_id=gv.CONN_ID_SNOWFLAKE,
        all_funds_table_name=ac.IN_ALL_FUNDS_TABLE_NAME,
        all_funds=all_funds_mask,
    )


extract_albourne_all_funds()
