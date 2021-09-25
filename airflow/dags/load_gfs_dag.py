from __future__ import print_function

import logging

from geoetl_airflow.build_load_gfs_dag import build_load_gfs_dag
from geoetl_airflow.variables import read_load_gfs_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

DAG = build_load_gfs_dag(
    dag_id="load_gfs_dag",
    **read_load_gfs_dag_vars(
        destination_table_name="NOAA_GFS0P25",
        load_schedule_interval="30 12 * * *",
        load_start_date="2021-06-01",
        load_max_active_runs=1,
    ),
)
