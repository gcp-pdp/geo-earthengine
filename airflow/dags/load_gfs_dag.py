from __future__ import print_function

import logging

from geoetl_airflow.build_load_dag import build_load_dag
from geoetl_airflow.utils.list_utils import flatten_ranges
from geoetl_airflow.variables import read_load_dag_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_dag(dag, forecast_hours_ranges):
    return build_load_dag(
        data_type="GFS",
        dag_id=f"load_{dag}_dag",
        forecast_hours=flatten_ranges(forecast_hours_ranges),
        **read_load_dag_vars(
            group=dag,
            destination_table_name="NOAA_GFS0P25",
            load_schedule_interval="30 12 * * *",
            load_start_date="2021-06-01T00:00:00",
            load_max_active_runs=1,
        ),
    )


DAG1 = build_dag("gfs_012", [range(3, 37, 3)])
DAG2 = build_dag("gfs_173", [range(1, 121, 1), range(123, 241, 3), range(252, 385, 12)])
DAG3 = build_dag("gfs_209", [range(1, 121, 1), range(123, 385, 3)])
