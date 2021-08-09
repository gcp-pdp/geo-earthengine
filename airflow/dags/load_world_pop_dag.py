from __future__ import print_function

import logging

from geoetl_airflow.build_load_dag import build_load_dag
from geoetl_airflow.variables import read_load_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_load_dag(
    dag_id='load_world_pop_dag',
    load_type='world_pop',
    **read_load_dag_vars(
        var_prefix='world_pop_',
        destination_table_name='world_pop',
        load_schedule_interval=None,
        load_max_active_runs=1,
        load_start_date='2020-01-01'
    )
)
