import os
from datetime import datetime, timedelta

from airflow import models, configuration
from airflow.contrib.sensors.gcs_sensor import (
    GoogleCloudStorageObjectSensor,
)
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)
from google.cloud import bigquery
from google.cloud.bigquery import RangePartitioning, PartitionRange

from geoetl_airflow.utils.bigquery_utils import (
    read_bigquery_schema_from_file,
    create_dataset,
    submit_bigquery_job,
    does_table_exist,
)

from geoetl_airflow.utils.file_utils import read_file


def build_load_worldpop_dag(
    dag_id,
    output_bucket,
    countries,
    large_countries,
    destination_dataset_project_id,
    destination_dataset_name,
    destination_table_name,
    staging_dataset_project_id,
    staging_dataset_name,
    dataflow_template_path,
    dataflow_environment,
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

    def read_bigquery_schema(schema):
        schema_path = os.path.join(
            dags_folder,
            "resources/stages/load/schemas/{schema}.json".format(schema=schema),
        )
        return read_bigquery_schema_from_file(schema_path)

    def load_task(country, **context):
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()
        job_config.schema = read_bigquery_schema("world_pop")
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.ignore_unknown_values = True
        job_config.range_partitioning = RangePartitioning(
            field="year",
            range_=PartitionRange(start=1900, end=2100, interval=1),
        )

        execution_date = context["execution_date"]
        load_table_name = "{table}_{country}_{year}".format(
            table=destination_table_name,
            country=country,
            year=execution_date.strftime("%Y"),
        )
        table_ref = create_dataset(
            client,
            staging_dataset_name,
            project=staging_dataset_project_id,
        ).table(load_table_name)

        load_uri = "gs://{bucket}/{prefix}/world_pop/year={year}/parquet/{country}_{year}.parquet".format(
            bucket=output_bucket,
            prefix=output_path_prefix,
            country=country,
            year=execution_date.strftime("%Y"),
        )
        load_job = client.load_table_from_uri(
            load_uri,
            table_ref,
            job_config=job_config,
        )
        submit_bigquery_job(load_job, job_config)
        assert load_job.state == "DONE"

    def merge_task(country, **context):
        client = bigquery.Client()

        table_ref = create_dataset(
            client,
            destination_dataset_name,
            project=destination_dataset_project_id,
        ).table(destination_table_name)
        if not does_table_exist(client, table_ref):
            table = bigquery.Table(table_ref, schema=read_bigquery_schema("world_pop"))
            table.range_partitioning = RangePartitioning(
                field="year",
                range_=PartitionRange(start=1900, end=2100, interval=1),
            )
            table.clustering_fields = [
                "geography",
                "geography_polygon",
                "country",
            ]
            client.create_table(table)

        job_config = bigquery.QueryJobConfig()
        job_config.priority = bigquery.QueryPriority.INTERACTIVE

        sql_path = os.path.join(
            dags_folder, "resources/stages/load/sqls/merge_worldpop.sql"
        )
        sql_template = read_file(sql_path)

        execution_date = context["execution_date"]
        year = execution_date.strftime("%Y")
        staging_table_name = "{table}_{country}_{year}".format(
            table=destination_table_name, country=country, year=year
        )

        template_context = {
            "year": year,
            "country": country,
            "source_table": staging_table_name,
            "source_project_id": staging_dataset_project_id,
            "source_dataset_name": staging_dataset_name,
            "destination_table": destination_table_name,
            "destination_dataset_project_id": destination_dataset_project_id,
            "destination_dataset_name": destination_dataset_name,
        }

        sql = context["task"].render_template(sql_template, template_context)
        job = client.query(sql, location="US", job_config=job_config)
        submit_bigquery_job(job, job_config)
        assert job.state == "DONE"

    priority = len(countries)
    for country in countries.split(","):
        c = country.lower()
        wait_uri = (
            "{prefix}/world_pop/year={year}/parquet/{country}_{year}.parquet".format(
                prefix=output_path_prefix,
                country=country,
                year='{{execution_date.strftime("%Y")}}',
            )
        )
        wait_gcs = GoogleCloudStorageObjectSensor(
            task_id=f"wait_{c}",
            timeout=60 * 60,
            poke_interval=60,
            bucket=output_bucket,
            object=wait_uri,
            weight_rule="upstream",
            priority_weight=priority,
            dag=dag,
        )
        if country in large_countries:
            input_file = "gs://{bucket}/{prefix}/world_pop/year={year}/parquet/{country}_{year}.parquet".format(
                bucket=output_bucket,
                prefix=output_path_prefix,
                country=country,
                year='{{ execution_date.strftime("%Y") }}',
            )
            output_table = "{table}_{country}_{year}".format(
                table=destination_table_name,
                country=country,
                year='{{ execution_date.strftime("%Y") }}',
            )
            load_operator = DataflowStartFlexTemplateOperator(
                task_id=f"run_dataflow_load_{c}",
                body={
                    "launchParameter": {
                        "containerSpecGcsPath": f"{dataflow_template_path}/load-parquet-0.1.0.json",
                        "jobName": "load-parquet-{country}".format(country=c)
                        + '-{{ execution_date.strftime("%Y%m%d-%H%M%S") }}',
                        "parameters": {
                            "input-file": input_file,
                            "output-table": f"{staging_dataset_project_id}:{staging_dataset_name}.{output_table}",
                            "output-schema": "/dataflow/template/schemas/world_pop.json",
                        },
                        "environment": dataflow_environment,
                    }
                },
                location="us-central1",
                wait_until_finished=True,
                dag=dag,
            )
        else:
            load_operator = PythonOperator(
                task_id=f"load_{c}",
                python_callable=load_task,
                execution_timeout=timedelta(minutes=600),
                provide_context=True,
                op_kwargs={"country": country},
                retries=1,
                retry_delay=timedelta(seconds=300),
                weight_rule="upstream",
                priority_weight=priority,
                dag=dag,
            )
        merge_operator = PythonOperator(
            task_id=f"merge_{c}",
            python_callable=merge_task,
            execution_timeout=timedelta(minutes=600),
            provide_context=True,
            op_kwargs={"country": country},
            retries=1,
            retry_delay=timedelta(seconds=300),
            weight_rule="upstream",
            priority_weight=priority,
            dag=dag,
        )
        priority -= 1
        wait_gcs >> load_operator >> merge_operator

    return dag
