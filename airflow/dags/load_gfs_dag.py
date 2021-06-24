from __future__ import print_function

import logging

from geoetl_airflow.build_load_dag import build_load_dag
from geoetl_airflow.variables import read_load_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# airflow DAG
DAG = build_load_dag(
    dag_id='load_gfs_dag',
    load_type='gfs',
    **read_load_dag_vars(
        var_prefix='gfs_',
        load_schedule_interval='0 4 * * *',
        load_max_active_runs=1,
    )
)
