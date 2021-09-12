from google.cloud.bigquery import RangePartitioning, PartitionRange

from geoetl_airflow.loader.base_loader import BaseLoader


class AnnualNPPLoader(BaseLoader):
    def __init__(self, **kwargs):
        super(AnnualNPPLoader, self).__init__(
            load_type="annual_npp",
            table_write_disposition="WRITE_TRUNCATE",
            table_partitioning=RangePartitioning(
                field="year",
                range_=PartitionRange(start=2000, end=3000, interval=1),
            ),
            **kwargs
        )

    def build_wait_uri(self):
        return "{prefix}/annual_npp/csv/".format(
            prefix=self.output_path_prefix,
        )

    def build_load_uri(self, execution_date):
        year = execution_date.strftime("%Y")
        return "gs://{bucket}/{prefix}/annual_npp/csv/{year}_*.csv".format(
            bucket=self.output_bucket, prefix=self.output_path_prefix, year=year
        )

    def load_table_name(self, execution_date):
        return "{table}${partition}".format(
            table=self.destination_table_name, partition=execution_date.strftime("%Y")
        )
