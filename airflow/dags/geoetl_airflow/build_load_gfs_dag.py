import logging
import os
from datetime import datetime, timedelta

from airflow import models, configuration
from airflow.contrib.sensors.gcs_sensor import (
    GoogleCloudStoragePrefixSensor,
)
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning

from geoetl_airflow.utils.bigquery_utils import (
    read_bigquery_schema_from_file,
    create_dataset,
    submit_bigquery_job,
    does_table_exist,
)
from geoetl_airflow.utils.file_utils import read_file


def build_load_gfs_dag(
    dag_id,
    output_bucket,
    destination_dataset_project_id,
    destination_dataset_name,
    destination_table_name,
    staging_dataset_project_id,
    staging_dataset_name,
    notification_emails=None,
    load_start_date=datetime(2000, 1, 1),
    load_schedule_interval="0 0 * * *",
    load_max_active_runs=None,
    load_concurrency=None,
    load_retries=5,
    load_retry_delay=300,
    output_path_prefix="export",
    **kwargs,
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

    dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")

    def load_task(**context):
        schema_path = os.path.join(
            dags_folder,
            "resources/stages/load/schemas/{schema}.json".format(schema="gfs"),
        )
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()
        job_config.schema = read_bigquery_schema_from_file(schema_path)
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.ignore_unknown_values = True
        job_config.time_partitioning = TimePartitioning(field="creation_time")

        execution_date = context["execution_date"]
        target_date = execution_date - timedelta(hours=4)
        load_table_name = "{table}_{suffix}".format(
            table=destination_table_name,
            suffix=target_date.strftime("%Y%m%d%H"),
        )
        table_ref = create_dataset(
            client,
            staging_dataset_name,
            project=staging_dataset_project_id,
        ).table(load_table_name)

        load_uri = "gs://{bucket}/{prefix}/gfs/date={execution_date}/{hour}/parquet/*.parquet".format(
            bucket=output_bucket,
            prefix=output_path_prefix,
            hour=target_date.strftime("%H"),
            execution_date=target_date.strftime("%Y-%m-%d"),
        )
        load_job = client.load_table_from_uri(
            load_uri,
            table_ref,
            job_config=job_config,
        )
        submit_bigquery_job(load_job, job_config)
        assert load_job.state == "DONE"

    def group_task(**context):
        client = bigquery.Client()
        schema_path = os.path.join(
            dags_folder,
            "resources/stages/load/schemas/{schema}.json".format(schema="gfs_group"),
        )
        table_ref = create_dataset(
            client,
            destination_dataset_name,
            project=destination_dataset_project_id,
        ).table(destination_table_name)
        if not does_table_exist(client, table_ref):
            logging.info(
                f"Creating table {destination_dataset_project_id}.{destination_dataset_name}.{destination_table_name}"
            )
            table = bigquery.Table(
                table_ref, schema=read_bigquery_schema_from_file(schema_path)
            )
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH, field="creation_time"
            )
            table.clustering_fields = ["geography"]
            client.create_table(table)

        job_config = bigquery.QueryJobConfig()
        job_config.priority = bigquery.QueryPriority.INTERACTIVE

        sql_path = os.path.join(dags_folder, "resources/stages/load/sqls/group_gfs.sql")
        sql_template = read_file(sql_path)

        execution_date = context["execution_date"]
        target_date = execution_date - timedelta(hours=4)
        source_table_name = "{table}_{suffix}".format(
            table=destination_table_name,
            suffix=target_date.strftime("%Y%m%d%H"),
        )

        creation_time = target_date.strftime("%Y-%m-%dT%H:00:00")
        logging.info(
            "Writing to table {table} and creation_time: {creation_time}".format(
                table=destination_table_name, creation_time=creation_time
            )
        )

        template_context = {
            "creation_time": creation_time,
            "source_project_id": staging_dataset_project_id,
            "source_dataset": staging_dataset_name,
            "source_table": source_table_name,
            "destination_table": destination_table_name,
            "destination_project_id": destination_dataset_project_id,
            "destination_dataset": destination_dataset_name,
        }

        sql = context["task"].render_template(sql_template, template_context)
        job = client.query(sql, location="US", job_config=job_config)
        submit_bigquery_job(job, job_config)
        assert job.state == "DONE"

    wait_export = ExternalTaskSensor(
        task_id="wait_export",
        timeout=60 * 60 * 3,
        poke_interval=60,
        external_dag_id="export_gfs_dag",
        external_task_id="export_gfs",
        execution_date_fn=lambda dt: dt.replace(minute=0, second=0),
        dag=dag,
    )
    wait_uri = "{prefix}/gfs/date={date}/{hour}/parquet/".format(
        prefix=output_path_prefix,
        hour='{{ (execution_date - macros.timedelta(hours=4)).strftime("%H") }}',
        date='{{ execution_date.strftime("%Y-%m-%d") }}',
    )
    wait_gcs = GoogleCloudStoragePrefixSensor(
        task_id="wait_gcs",
        timeout=60 * 60,
        poke_interval=60,
        bucket=output_bucket,
        prefix=wait_uri,
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
    group_operator = PythonOperator(
        task_id="group_by_time_geography",
        python_callable=group_task,
        provide_context=True,
        execution_timeout=timedelta(minutes=60),
        retries=1,
        retry_delay=timedelta(seconds=300),
        dag=dag,
    )
    wait_export >> wait_gcs >> load_operator >> group_operator
    return dag
