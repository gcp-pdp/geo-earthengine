from datetime import timedelta

from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning

from geoetl_airflow.loader.base_loader import BaseLoader
from geoetl_airflow.utils.bigquery_utils import (
    create_dataset,
    submit_bigquery_job,
    does_table_exist,
)
import os

from geoetl_airflow.utils.file_utils import read_file


class GFSLoader(BaseLoader):
    def __init__(self, **kwargs):
        super(GFSLoader, self).__init__(
            table_schema_name="gfs",
            table_write_disposition="WRITE_APPEND",
            table_partitioning=TimePartitioning(field="creation_time"),
            **kwargs,
        )
        self.temp_dataset_name = f"{self.destination_dataset_name}_temp"

    def add_load_tasks(self, dag):
        max_priority = 10000
        group_operator = PythonOperator(
            task_id="group_gfs",
            python_callable=self.group_task,
            provide_context=True,
            execution_timeout=timedelta(minutes=30),
            retries=3,
            retry_delay=timedelta(minutes=5),
            priority_weight=max_priority,
            dag=dag,
        )

        for i, (task_id, wait_uri, load_uri) in enumerate(self.task_config()):
            wait_task_concurrency = self.load_concurrency / 2
            wait_task_priority = max_priority - (i * 10)
            wait_sensor = GoogleCloudStorageObjectSensor(
                task_id="wait_{task_id}".format(task_id=task_id),
                task_concurrency=wait_task_concurrency,
                timeout=60 * 60,
                poke_interval=60,
                bucket=self.output_bucket,
                object=wait_uri,
                priority_weight=wait_task_priority,
                dag=dag,
            )
            load_operator = PythonOperator(
                task_id="load_{task_id}".format(task_id=task_id),
                weight_rule="upstream",
                python_callable=self.load_task,
                execution_timeout=timedelta(minutes=30),
                op_kwargs={"uri": load_uri},
                provide_context=True,
                priority_weight=max_priority - (i * 10) + 1,
                dag=dag,
            )
            wait_sensor >> load_operator >> group_operator

        return group_operator

    def load_task(self, uri, **context):
        client = bigquery.Client()
        job_config = self.create_job_config()
        table_ref = create_dataset(
            client,
            self.temp_dataset_name,
            project=self.destination_dataset_project_id,
        ).table(self.temp_table(context["execution_date"]))
        load_job = client.load_table_from_uri(
            self.build_load_uri(uri, context["execution_date"]),
            table_ref,
            job_config=job_config,
        )
        submit_bigquery_job(load_job, job_config)
        assert load_job.state == "DONE"

    def group_task(self, **context):
        client = bigquery.Client()
        dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")

        table_ref = create_dataset(
            client,
            self.destination_dataset_name,
            project=self.destination_dataset_project_id,
        ).table(self.destination_table_name)
        if not does_table_exist(client, table_ref):
            client.create_table(
                bigquery.Table(table_ref, schema=self.load_schema("gfs_group"))
            )

        job_config = bigquery.QueryJobConfig()
        job_config.priority = bigquery.QueryPriority.INTERACTIVE

        sql_path = os.path.join(dags_folder, "resources/stages/load/sqls/group_gfs.sql")
        sql_template = read_file(sql_path)

        template_context = {
            "table": self.destination_table_name,
            "table_temp": self.temp_table(context["execution_date"]),
            "project_id": self.destination_dataset_project_id,
            "dataset": self.destination_dataset_name,
            "dataset_temp": self.temp_dataset_name,
        }

        sql = context["task"].render_template(sql_template, template_context)
        job = client.query(sql, location="US", job_config=job_config)
        submit_bigquery_job(job, job_config)
        assert job.state == "DONE"

    def task_config(self):
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

    def temp_table(self, execution_date):
        date = self.target_date(execution_date)
        return "{table}_{date}".format(
            table=self.destination_table_name, date=date.strftime("%Y%m%d%H")
        )

    def target_date(self, execution_date):
        return execution_date - timedelta(hours=4)
