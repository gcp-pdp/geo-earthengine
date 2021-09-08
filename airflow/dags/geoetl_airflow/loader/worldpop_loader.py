from google.cloud.bigquery import RangePartitioning, PartitionRange

from geoetl_airflow.loader.base_loader import BaseLoader


class WorldPopLoader(BaseLoader):
    def __init__(self, **kwargs):
        super(WorldPopLoader, self).__init__(
            table_schema_name="world_pop",
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

    def task_config(self):
        wait_uri = "{prefix}/world_pop/year={year}/csv/".format(
            prefix=self.output_path_prefix,
            year='{{execution_date.strftime("%Y")}}',
        )
        load_uri = "gs://{bucket}/{prefix}/world_pop/year={{year}}/csv/*.csv".format(
            bucket=self.output_bucket,
            prefix=self.output_path_prefix,
        )
        return "world_pop", wait_uri, load_uri
