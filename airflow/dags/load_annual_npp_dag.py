from __future__ import print_function

import logging

from geoetl_airflow.build_load_dag import build_load_dag
from geoetl_airflow.variables import read_load_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_load_dag(
    dag_id='load_annual_npp_dag',
    load_type='annual_npp',
    **read_load_dag_vars(
        var_prefix='annual_npp_',
        load_schedule_interval=None,
        load_max_active_runs=1,
        load_start_date='2000-01-01'
    )
)
