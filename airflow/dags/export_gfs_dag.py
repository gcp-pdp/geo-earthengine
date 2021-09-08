from __future__ import print_function

from geoetl_airflow.build_export_dag import build_export_dag
from geoetl_airflow.variables import read_export_dag_vars

DAG = build_export_dag(
    data_type="GFS",
    dag_id="export_gfs_dag",
    **read_export_dag_vars(
        group="gfs",
        export_schedule_interval="30 0/6 * * *",
        export_start_date="2021-06-01",
        export_max_active_runs=1,
        export_concurrency=4,
    )
)
