import logging
import os
from datetime import timedelta

from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning

from geoetl_airflow.loader.base_loader import BaseLoader
from geoetl_airflow.utils.bigquery_utils import (
    create_dataset,
    submit_bigquery_job,
    does_table_exist,
)
from geoetl_airflow.utils.file_utils import read_file


class GFSLoader(BaseLoader):
    def __init__(self, **kwargs):
        super(GFSLoader, self).__init__(
            load_type="gfs",
            table_write_disposition="WRITE_TRUNCATE",
            table_partitioning=TimePartitioning(field="creation_time"),
            **kwargs,
        )

    def add_aggregate_task(self, dag, upstream_task):
        group_operator = PythonOperator(
            task_id="group_by_time_geography",
            python_callable=self.group_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=60),
            retries=1,
            retry_delay=timedelta(seconds=300),
            dag=dag,
        )

        return upstream_task >> group_operator

    def load_table_name(self, execution_date):
        return "{table}_{suffix}".format(
            table=self.destination_table_name,
            suffix=self.target_date(execution_date).strftime("%Y%m%d%H"),
        )

    def group_task(self, **context):
        client = bigquery.Client()
        dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")

        table_ref = create_dataset(
            client,
            self.destination_dataset_name,
            project=self.destination_dataset_project_id,
        ).table(self.destination_table_name)
        if not does_table_exist(client, table_ref):
            table = bigquery.Table(table_ref, schema=self.read_schema("gfs_group"))
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH, field="creation_time"
            )
            table.clustering_fields = ["geography"]
            client.create_table(table)

        job_config = bigquery.QueryJobConfig()
        job_config.priority = bigquery.QueryPriority.INTERACTIVE

        sql_path = os.path.join(dags_folder, "resources/stages/load/sqls/group_gfs.sql")
        sql_template = read_file(sql_path)

        creation_time = self.target_date(context["execution_date"]).strftime(
            "%Y-%m-%dT%H:00:00"
        )
        logging.info(
            "Writing to table {table} and creation_time: {creation_time}".format(
                table=self.destination_table_name, creation_time=creation_time
            )
        )

        template_context = {
            "creation_time": creation_time,
            "source_project_id": self.load_dataset_project_id,
            "source_dataset": self.load_dataset_name,
            "source_table": self.load_table_name(context["execution_date"]),
            "destination_table": self.destination_table_name,
            "destination_project_id": self.destination_dataset_project_id,
            "destination_dataset": self.destination_dataset_name,
        }

        sql = context["task"].render_template(sql_template, template_context)
        job = client.query(sql, location="US", job_config=job_config)
        submit_bigquery_job(job, job_config)
        assert job.state == "DONE"

    def build_wait_uri(self):
        return "{prefix}/gfs/date={date}/{hour}/csv/".format(
            prefix=self.output_path_prefix,
            hour='{{ (execution_date - macros.timedelta(hours=4)).strftime("%H") }}',
            date='{{ execution_date.strftime("%Y-%m-%d") }}',
        )

    def build_load_uri(self, execution_date):
        return (
            "gs://{bucket}/{prefix}/gfs/date={execution_date}/{hour}/csv/*.csv".format(
                bucket=self.output_bucket,
                prefix=self.output_path_prefix,
                hour=self.target_date(execution_date).strftime("%H"),
                execution_date=self.target_date(execution_date).strftime("%Y-%m-%d"),
            )
        )

    @staticmethod
    def target_date(execution_date):
        return execution_date - timedelta(hours=4)
