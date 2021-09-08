import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from airflow import models, configuration
from airflow.contrib.sensors.gcs_sensor import (
    GoogleCloudStoragePrefixSensor,
)
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning, RangePartitioning

from geoetl_airflow.utils.bigquery_utils import (
    read_bigquery_schema_from_file,
    create_dataset,
    submit_bigquery_job,
)


class BaseLoader(ABC):
    def __init__(
        self,
        dag_id,
        output_bucket,
        destination_dataset_project_id,
        destination_dataset_name,
        destination_table_name,
        notification_emails=None,
        load_start_date=datetime(2021, 5, 1),
        load_end_date=None,
        load_schedule_interval="0 0 * * *",
        load_max_active_runs=None,
        load_concurrency=None,
        load_retries=5,
        load_retry_delay=300,
        output_path_prefix="export",
        table_schema_name=None,
        table_clustering_fields=None,
        table_partitioning=None,
        table_write_disposition="WRITE_TRUNCATE",
    ):
        self.dag_id = dag_id
        self.output_bucket = output_bucket
        self.destination_dataset_project_id = destination_dataset_project_id
        self.destination_dataset_name = destination_dataset_name
        self.destination_table_name = destination_table_name
        self.notification_emails = notification_emails
        self.load_start_date = load_start_date
        self.load_end_date = load_end_date
        self.load_schedule_interval = load_schedule_interval
        self.load_max_active_runs = load_max_active_runs
        self.load_concurrency = load_concurrency
        self.load_retries = load_retries
        self.load_retry_delay = load_retry_delay
        self.output_path_prefix = output_path_prefix
        self.table_schema_name = table_schema_name
        self.table_clustering_fields = table_clustering_fields
        self.table_partitioning = table_partitioning
        self.table_write_disposition = table_write_disposition

    def build_dag(self):
        if not self.output_bucket:
            raise ValueError("output_bucket is required")
        if not self.destination_dataset_project_id:
            raise ValueError("destination_dataset_project_id is required")
        if not self.destination_dataset_name:
            raise ValueError("destination_dataset_name is required")
        if not self.destination_table_name:
            raise ValueError("destination_table_name is required")

        default_dag_args = {
            "depends_on_past": False,
            "start_date": self.load_start_date,
            "end_date": self.load_end_date,
            "email_on_failure": True,
            "email_on_retry": False,
            "retries": self.load_retries,
            "retry_delay": timedelta(seconds=self.load_retry_delay),
        }

        if self.notification_emails and len(self.notification_emails) > 0:
            default_dag_args["email"] = [
                email.strip() for email in self.notification_emails.split(",")
            ]

        if self.load_max_active_runs is None:
            self.load_max_active_runs = configuration.conf.getint(
                "core", "max_active_runs_per_dag"
            )

        dag = models.DAG(
            self.dag_id,
            schedule_interval=self.load_schedule_interval,
            max_active_runs=self.load_max_active_runs,
            concurrency=self.load_concurrency,
            default_args=default_dag_args,
        )

        load_task = self.add_load_tasks(dag)
        return dag

    def add_load_tasks(self, dag):
        (task_id, wait_uri, load_uri) = self.task_config()
        wait_sensor = GoogleCloudStoragePrefixSensor(
            task_id="wait_{task_id}".format(task_id=task_id),
            timeout=60 * 60,
            poke_interval=60,
            bucket=self.output_bucket,
            prefix=wait_uri,
            priority_weight=1,
            dag=dag,
        )
        load_operator = PythonOperator(
            task_id="load_{task_id}".format(task_id=task_id),
            weight_rule="upstream",
            python_callable=self.load_task,
            execution_timeout=timedelta(minutes=30),
            op_kwargs={"uri": load_uri},
            provide_context=True,
            priority_weight=2,
            dag=dag,
        )
        wait_sensor >> load_operator
        return load_operator

    def load_task(self, uri, **context):
        client = bigquery.Client()
        job_config = self.create_job_config()
        table_ref = create_dataset(
            client,
            self.destination_dataset_name,
            project=self.destination_dataset_project_id,
        ).table(self.destination_table_partition(context["execution_date"]))
        load_job = client.load_table_from_uri(
            self.build_load_uri(uri, context["execution_date"]),
            table_ref,
            job_config=job_config,
        )
        submit_bigquery_job(load_job, job_config)
        assert load_job.state == "DONE"

    def create_job_config(self):
        job_config = bigquery.LoadJobConfig()
        job_config.schema = self.load_schema(self.table_schema_name)
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.write_disposition = self.table_write_disposition
        job_config.ignore_unknown_values = True
        if self.table_clustering_fields is not None:
            job_config.clustering_fields = self.table_clustering_fields
        if type(self.table_partitioning) == TimePartitioning:
            job_config.time_partitioning = self.table_partitioning
        elif type(self.table_partitioning) == RangePartitioning:
            job_config.range_partitioning = self.table_partitioning
        return job_config

    def load_schema(self, schema_name):
        dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")
        schema_path = os.path.join(
            dags_folder,
            "resources/stages/load/schemas/{schema}.json".format(schema=schema_name),
        )
        return read_bigquery_schema_from_file(schema_path)

    def build_load_uri(self, uri, execution_date):
        year = execution_date.strftime("%Y")
        return uri.format(year=year)

    def destination_table_partition(self, execution_date):
        year = execution_date.strftime("%Y")
        return "{table}${partition}".format(
            table=self.destination_table_name, partition=year
        )

    @abstractmethod
    def task_config(self):
        pass
