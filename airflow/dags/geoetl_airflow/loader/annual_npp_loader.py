from google.cloud.bigquery import RangePartitioning, PartitionRange

from geoetl_airflow.loader.base_loader import BaseLoader


class AnnualNPPLoader(BaseLoader):
    def __init__(self, **kwargs):
        super(AnnualNPPLoader, self).__init__(
            table_schema_name="annual_npp",
            table_write_disposition="WRITE_TRUNCATE",
            table_partitioning=RangePartitioning(
                field="year",
                range_=PartitionRange(start=2000, end=3000, interval=1),
            ),
            **kwargs
        )

    def tasks(self):
        wait_uri = "{prefix}/annual_npp/csv/".format(
            prefix=self.output_path_prefix,
            year='{{execution_date.strftime("%Y")}}',
        )
        load_uri = "gs://{bucket}/{prefix}/annual_npp/csv/{{year}}_*.csv".format(
            bucket=self.output_bucket,
            prefix=self.output_path_prefix,
        )
        yield "annual_npp", wait_uri, load_uri
