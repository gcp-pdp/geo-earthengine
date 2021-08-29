from __future__ import print_function

import logging

from geoetl_airflow.build_load_dag import build_load_dag
from geoetl_airflow.variables import read_load_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_load_dag(
    data_type="WORLD_POP",
    dag_id="load_world_pop_dag",
    **read_load_dag_vars(
        group="world_pop",
        destination_table_name="world_pop",
        load_schedule_interval=None,
        load_max_active_runs=1,
        load_start_date="2020-01-01",
    )
)
