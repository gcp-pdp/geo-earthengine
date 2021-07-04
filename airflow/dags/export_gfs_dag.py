from __future__ import print_function

from geoetl_airflow.build_export_dag import build_export_dag
from geoetl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_export_dag(
    dag_id='export_gfs_dag',
    export_type='gfs',
    **read_export_dag_vars(
        var_prefix='gfs_',
        export_schedule_interval='30 0/6 * * *',
        export_start_date='2021-06-01',
        export_max_active_runs=3,
    )
)
