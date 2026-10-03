"""DAG that runs a transformation on data in DuckDB"""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import constants as c

# ------- #
# Queries #
# ------- #


# run a SQL transformation on the 'in_climate' table in order to create averages
# over different time periods
@task(pool="duckdb")
def create_global_climate_reporting_table(
    duckdb_conn_id: str, in_table_name: str, output_table_name: str
):
    from duckdb_provider.hooks.duckdb_hook import DuckDBHook

    duckdb_conn = DuckDBHook(duckdb_conn_id).get_conn()
    cursor = duckdb_conn.cursor()
    cursor.sql(
        f"""
        CREATE OR REPLACE TABLE {output_table_name} AS
        SELECT CAST(dt AS DATE) AS date,
        AVG(LandAverageTemperature) OVER(PARTITION BY YEAR(CAST(dt AS DATE))/10*10) AS decade_average_temp,
        AVG(LandAverageTemperature) OVER(PARTITION BY YEAR(CAST(dt AS DATE))) AS year_average_temp,
        AVG(LandAverageTemperature) OVER(PARTITION BY MONTH(CAST(dt AS DATE))) AS month_average_temp,
        AVG(LandAverageTemperature) OVER(PARTITION BY CAST(dt AS DATE)) AS day_average_temp,
        FROM {in_table_name}
        """
    )
    cursor.close()


# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in DuckDB
    schedule=[Dataset("duckdb://include/dwh/in_climate")],
    catchup=False,
    default_args=gv.default_args,
    description="Runs a transformation on climate data in DuckDB.",
    tags=["part_1", "quick start"],
)
def transform_climate_data():

    # input the raw climate data and save the outcome of the transformation to a
    # permanent reporting table
    create_global_climate_reporting_table(
        duckdb_conn_id=gv.CONN_ID_DUCKDB,
        in_table_name=c.IN_CLIMATE_TABLE_NAME,
        output_table_name=c.REPORT_CLIMATE_TABLE_NAME,
    )


transform_climate_data()
