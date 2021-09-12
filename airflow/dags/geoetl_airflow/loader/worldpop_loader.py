from google.cloud.bigquery import RangePartitioning, PartitionRange

from geoetl_airflow.loader.base_loader import BaseLoader


class WorldPopLoader(BaseLoader):
    def __init__(self, **kwargs):
        super(WorldPopLoader, self).__init__(
            load_type="world_pop",
            table_write_disposition="WRITE_TRUNCATE",
            table_clustering_fields=[
                "geography",
                "geography_polygon",
                "country",
            ],
            table_partitioning=RangePartitioning(
                field="year",
                range_=PartitionRange(start=2000, end=3000, interval=1),
            ),
            **kwargs
        )

    def build_wait_uri(self):
        return "{prefix}/world_pop/year={year}/csv/".format(
            prefix=self.output_path_prefix,
            year='{{execution_date.strftime("%Y")}}',
        )

    def build_load_uri(self, execution_date):
        year = execution_date.strftime("%Y")
        return "gs://{bucket}/{prefix}/world_pop/year={year}/csv/*.csv".format(
            bucket=self.output_bucket, prefix=self.output_path_prefix, year=year
        )

    def load_table_name(self, execution_date):
        return "{table}${partition}".format(
            table=self.destination_table_name, partition=execution_date.strftime("%Y")
        )
