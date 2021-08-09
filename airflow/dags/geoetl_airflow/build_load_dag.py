from __future__ import print_function

import logging
import os
from datetime import datetime, timedelta

from airflow import models, configuration
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning, RangePartitioning, PartitionRange

from geoetl_airflow.bigquery_utils import submit_bigquery_job, create_dataset, read_bigquery_schema_from_file

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def build_load_dag(
        dag_id,
        load_type,
        output_bucket,
        destination_dataset_project_id,
        destination_dataset_name,
        destination_table_name,
        notification_emails=None,
        load_start_date=datetime(2021, 5, 1),
        load_end_date=None,
        load_schedule_interval='0 0 * * *',
        load_max_active_runs=None,
        output_path_prefix='export',
):
    """Build Load DAG"""

    if not output_bucket:
        raise ValueError('output_bucket is required')
    if not destination_dataset_project_id:
        raise ValueError('destination_dataset_project_id is required')
    if not destination_dataset_name:
        raise ValueError('destination_dataset_name is required')
    if not destination_table_name:
        raise ValueError('destination_table_name is required')

    default_dag_args = {
        'depends_on_past': False,
        'start_date': load_start_date,
        'end_date': load_end_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    if load_max_active_runs is None:
        load_max_active_runs = configuration.conf.getint('core', 'max_active_runs_per_dag')

    environment = {
        'dataset_name': destination_dataset_name,
        'destination_dataset_project_id': destination_dataset_project_id
    }

    dag = models.DAG(
        dag_id,
        catchup=False if load_start_date is None else True,
        schedule_interval=load_schedule_interval,
        max_active_runs=load_max_active_runs,
        default_args=default_dag_args)

    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

    def add_load_tasks(task):
        if task == 'gfs':
            output_prefix = '{path_prefix}/{task}/date={date}/csv/'\
                .format(path_prefix=output_path_prefix,
                        task=task,
                        date='{{ (execution_date - macros.timedelta(hours=9)).strftime("%Y-%m-%d") }}')
        elif task == 'world_pop':
            output_prefix = '{path_prefix}/{task}/year={year}/csv/'\
                .format(path_prefix=output_path_prefix, task=task, year='{{execution_date.strftime("%Y")}}')
        elif task == 'annual_npp':
            output_prefix = '{path_prefix}/{task}/csv/'.format(path_prefix=output_path_prefix, task=task)

        wait_sensor = GoogleCloudStoragePrefixSensor(
            task_id='wait_{task}'.format(task=task),
            timeout=60 * 60,
            poke_interval=60,
            bucket=output_bucket,
            prefix=output_prefix,
            dag=dag
        )

        def load_task(**context):
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig()
            schema_path = os.path.join(dags_folder, 'resources/stages/load/schemas/{task}.json'.format(task=task))
            job_config.schema = read_bigquery_schema_from_file(schema_path)
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.write_disposition = 'WRITE_TRUNCATE'
            job_config.ignore_unknown_values = True

            if load_type == 'gfs':
                date = context['execution_date'] - timedelta(hours=9)
                uri = 'gs://{bucket}/{path_prefix}/{task}/date={date}/csv/*.csv'.format(
                    bucket=output_bucket,
                    path_prefix=output_path_prefix,
                    date=date.strftime('%Y-%m-%d'),
                    task=task)
                table = '{table}${partition}'.format(
                    table=destination_table_name,
                    partition=date.strftime('%Y%m%d')
                )
                job_config.time_partitioning = TimePartitioning(field='creation_time')

            elif load_type == 'world_pop':
                year = context['execution_date'].strftime('%Y')
                uri = 'gs://{bucket}/{path_prefix}/{task}/year={year}/csv/*.csv'.format(
                    bucket=output_bucket,
                    path_prefix=output_path_prefix,
                    year=year,
                    task=task)
                table = '{table}${partition}'.format(
                    table=destination_table_name,
                    partition=year
                )
                job_config.range_partitioning = RangePartitioning(
                    field='year',
                    range_=PartitionRange(start=2000, end=3000, interval=1)
                )
                job_config.clustering_fields = ['geography', 'geography_polygon', 'country']

            elif load_type == 'annual_npp':
                year = context['execution_date'].strftime('%Y')
                uri = 'gs://{bucket}/{path_prefix}/{task}/csv/{year}_*.csv'.format(
                    bucket=output_bucket,
                    path_prefix=output_path_prefix,
                    year=year,
                    task=task)
                table = '{table}${partition}'.format(
                    table=destination_table_name,
                    partition=year
                )
                job_config.range_partitioning = RangePartitioning(
                    field='year',
                    range_=PartitionRange(start=2000, end=3000, interval=1)
                )

            table_ref = create_dataset(client, destination_dataset_name).table(table)
            load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
            submit_bigquery_job(load_job, job_config)
            assert load_job.state == 'DONE'

        load_operator = PythonOperator(
            task_id='load_{task}'.format(task=task),
            python_callable=load_task,
            execution_timeout=timedelta(minutes=30),
            provide_context=True,
            dag=dag
        )

        wait_sensor >> load_operator
        return load_operator

    load_task = add_load_tasks(load_type)

    return dag
