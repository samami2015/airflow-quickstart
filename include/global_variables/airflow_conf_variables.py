# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
import logging
import os
from pendulum import duration
import json

# ----------------------- #
# Configuration variables #
# ----------------------- #

# Source files climate data
CLIMATE_DATA_PATH = (
    f"{os.environ['AIRFLOW_HOME']}/include/climate_data/global_climate_data.csv"
)

# SQL files
TEMPLATE_SQL_PATH = (
    f"{os.environ['AIRFLOW_HOME']}/include/sql"
)

# Datasets
DS_START = Dataset("start")


# DuckDB config
CONN_ID_DUCKDB = "duckdb_default"
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]

# get Airflow task logger
task_log = logging.getLogger("airflow.task")

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(minutes=1),
}

# default coordinates
default_coordinates = {"city": "No city provided", "lat": 0, "long": 0}

# SnowFlake config
CONN_ID_SNOWFLAKE = "snowflake_conn"

# HTTP config
CONN_ID_HTTP = "http_conn_albourne"
