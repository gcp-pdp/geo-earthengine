from __future__ import print_function

from geoetl_airflow.build_export_npp_dag import build_export_npp_dag
from geoetl_airflow.variables import read_export_npp_dag_vars

DAG = build_export_npp_dag(
    dag_id="export_annual_npp_dag",
    **read_export_npp_dag_vars(
        export_schedule_interval=None,
        export_start_date="2020-01-01",
        export_max_active_runs=1,
    )
)
