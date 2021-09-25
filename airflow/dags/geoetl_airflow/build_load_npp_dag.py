import os
from datetime import datetime, timedelta

from airflow import models, configuration
from airflow.contrib.sensors.gcs_sensor import (
    GoogleCloudStorageObjectSensor,
)
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery import RangePartitioning, PartitionRange

from geoetl_airflow.utils.bigquery_utils import (
    read_bigquery_schema_from_file,
    create_dataset,
    submit_bigquery_job,
)


def build_load_npp_dag(
    dag_id,
    output_bucket,
    destination_dataset_project_id,
    destination_dataset_name,
    destination_table_name,
    notification_emails=None,
    load_start_date=datetime(2000, 1, 1),
    load_schedule_interval=None,
    load_max_active_runs=None,
    load_concurrency=None,
    load_retries=5,
    load_retry_delay=300,
    output_path_prefix="export",
    **kwargs
):
    if not output_bucket:
        raise ValueError("output_bucket is required")
    if not destination_dataset_project_id:
        raise ValueError("destination_dataset_project_id is required")
    if not destination_dataset_name:
        raise ValueError("destination_dataset_name is required")
    if not destination_table_name:
        raise ValueError("destination_table_name is required")

    default_dag_args = {
        "depends_on_past": False,
        "start_date": load_start_date,
        "end_date": None,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": load_retries,
        "retry_delay": timedelta(seconds=load_retry_delay),
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args["email"] = [
            email.strip() for email in notification_emails.split(",")
        ]

    if load_max_active_runs is None:
        load_max_active_runs = configuration.conf.getint(
            "core", "max_active_runs_per_dag"
        )

    dag = models.DAG(
        dag_id,
        schedule_interval=load_schedule_interval,
        max_active_runs=load_max_active_runs,
        concurrency=load_concurrency,
        default_args=default_dag_args,
        is_paused_upon_creation=True,
    )

    def load_task(**context):
        dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")
        schema_path = os.path.join(
            dags_folder,
            "resources/stages/load/schemas/{schema}.json".format(schema="annual_npp"),
        )
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()
        job_config.schema = read_bigquery_schema_from_file(schema_path)
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.ignore_unknown_values = True
        job_config.clustering_fields = [
            "geography",
            "geography_polygon",
        ]
        job_config.range_partitioning = RangePartitioning(
            field="year",
            range_=PartitionRange(start=1900, end=2100, interval=1),
        )
        execution_date = context["execution_date"]
        load_table_name = "{table}${partition}".format(
            table=destination_table_name, partition=execution_date.strftime("%Y")
        )
        table_ref = create_dataset(
            client,
            destination_dataset_name,
            project=destination_dataset_project_id,
        ).table(load_table_name)

        load_uri = "gs://{bucket}/{prefix}/annual_npp/parquet/{date}.parquet".format(
            bucket=output_bucket,
            prefix=output_path_prefix,
            date=execution_date.strftime("%Y_%m_%d"),
        )
        load_job = client.load_table_from_uri(
            load_uri,
            table_ref,
            job_config=job_config,
        )
        submit_bigquery_job(load_job, job_config)
        assert load_job.state == "DONE"

    wait_uri = "{prefix}/annual_npp/parquet/{date}.parquet".format(
        prefix=output_path_prefix, date='{{ execution_date.strftime("%Y_%m_%d") }}'
    )
    wait_gcs = GoogleCloudStorageObjectSensor(
        task_id="wait_gcs",
        timeout=60 * 60,
        poke_interval=60,
        bucket=output_bucket,
        object=wait_uri,
        dag=dag,
    )
    load_operator = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_task,
        execution_timeout=timedelta(minutes=600),
        provide_context=True,
        retries=1,
        retry_delay=timedelta(seconds=300),
        dag=dag,
    )
    wait_gcs >> load_operator
    return dag
