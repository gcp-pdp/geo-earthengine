from __future__ import print_function

from geoetl_airflow.build_export_dag import build_export_dag
from geoetl_airflow.variables import read_export_dag_vars

DAG = build_export_dag(
    data_type="WORLD_POP",
    dag_id="export_world_pop_dag",
    **read_export_dag_vars(
        group="world_pop",
        export_schedule_interval=None,
        export_start_date="2020-01-01T00:00:00",
        export_max_active_runs=1,
    )
)
