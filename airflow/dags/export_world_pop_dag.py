from __future__ import print_function

from geoetl_airflow.build_export_dag import build_export_dag
from geoetl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_export_dag(
    dag_id='export_world_pop_dag',
    export_type='world_pop',
    **read_export_dag_vars(
        var_prefix='world_pop_',
        export_schedule_interval=None,
        export_start_date='2020-01-01',
        export_max_active_runs=1,
    )
)
