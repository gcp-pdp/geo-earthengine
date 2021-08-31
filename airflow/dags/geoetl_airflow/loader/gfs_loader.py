from datetime import timedelta

from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from google.cloud.bigquery import TimePartitioning

from geoetl_airflow.loader.base_loader import BaseLoader


class GFSLoader(BaseLoader):
    def __init__(self, **kwargs):
        super(GFSLoader, self).__init__(
            table_schema_name="gfs",
            table_write_disposition="WRITE_APPEND",
            table_partitioning=TimePartitioning(field="creation_time"),
            **kwargs
        )

    def tasks(self):
        for i in list(range(1, 121)) + list(range(123, 385, 3)):
            wait_uri = "{prefix}/gfs/date={date}/csv/{file}".format(
                prefix=self.output_path_prefix,
                date='{{ execution_date.strftime("%Y-%m-%d") }}',
                file='{{ (execution_date - macros.timedelta(hours=4)).strftime("%Y%m%d%H") }}F'
                + str(i).zfill(3)
                + ".csv",
            )
            load_uri = (
                "gs://{bucket}/{prefix}/gfs/date={{execution_date}}/csv/{file}".format(
                    bucket=self.output_bucket,
                    prefix=self.output_path_prefix,
                    file="{{execution_time}}F{interval:03d}.csv".format(interval=i),
                )
            )
            yield ("gfs_{interval}".format(interval=i), wait_uri, load_uri)

    def build_load_uri(self, uri, execution_date):
        date = self.target_date(execution_date)
        return uri.format(
            execution_date=date.strftime("%Y-%m-%d"),
            execution_time=date.strftime("%Y%m%d%H"),
        )

    def destination_table_partition(self, execution_date):
        date = self.target_date(execution_date)
        return "{table}${partition}".format(
            table=self.destination_table_name, partition=date.strftime("%Y%m%d")
        )

    def target_date(self, execution_date):
        return execution_date - timedelta(hours=4)
