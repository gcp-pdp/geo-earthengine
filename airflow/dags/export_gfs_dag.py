from __future__ import print_function

from geoetl_airflow.build_export_dag import build_export_dag
from geoetl_airflow.utils.list_utils import flatten_ranges
from geoetl_airflow.variables import read_export_dag_vars


def build_dag(dag, forecast_hours_ranges):
    return build_export_dag(
        data_type="GFS",
        dag_id=f"export_{dag}_dag",
        forecast_hours=flatten_ranges(forecast_hours_ranges),
        **read_export_dag_vars(
            group=dag,
            export_schedule_interval="30 0/6 * * *",
            export_start_date="2021-06-01T00:00:00",
            export_max_active_runs=1,
            export_concurrency=4,
        ),
    )


DAG1 = build_dag("gfs_012", [range(3, 37, 3)])
DAG2 = build_dag("gfs_173", [range(1, 121, 1), range(123, 241, 3), range(252, 385, 12)])
DAG3 = build_dag("gfs_209", [range(1, 121, 1), range(123, 385, 3)])
