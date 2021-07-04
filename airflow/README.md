# Geo Weather ETL Airflow

Airflow DAGs for exporting and loading the geo weather data to Google BigQuery:

- [export_gfs_dag.py](dags/export_gfs_dag.py) - exports geo weather data to a GCS bucket.
- [load_gfs_dag.py](dags/load_gfs_dag.py) - loads geo weather data from GCS bucket to BigQuery.
- [export_world_pop_dag.py](dags/export_world_pop_dag.py) - exports human population data to a GCS bucket.
- [load_world_pop_dag.py](dags/load_gfs_dag.py) - loads human population data from GCS bucket to BigQuery.
- [export_annual_npp_dag.py](dags/export_annual_npp_dag.py) - exports annual NPP data to a GCS bucket.
- [load_annual_npp_dag.py](dags/load_annual_npp_dag.py) - loads annual NPP data from GCS bucket to BigQuery.

## Prerequisites

* linux/macos terminal 
* git
* [gcloud](https://cloud.google.com/sdk/install)

## Setting Up

1. Create a GCS bucket to hold export files:

    ```bash
    gcloud config set project <your_gcp_project>
    PROJECT=$(gcloud config get-value project 2> /dev/null)
    ENVIRONMENT_INDEX=0
    BUCKET=${PROJECT}-${ENVIRONMENT_INDEX}
    gsutil mb gs://${BUCKET}/
    ```

2. Create a Google Cloud Composer environment:

    ```bash
    ENVIRONMENT_NAME=${PROJECT}-${ENVIRONMENT_INDEX} && echo "Environment name is ${ENVIRONMENT_NAME}"
    gcloud composer environments create ${ENVIRONMENT_NAME} --location=us-central1 --zone=us-central1-a \
        --disk-size=30GB --machine-type=n1-standard-1 --node-count=3 --python-version=3 --image-version=composer-1.16.5-airflow-1.10.14 \
        --network=default --subnetwork=default    
    ```
   
    Note that if Composer API is not enabled the command above will auto prompt to enable it.
   
3. [Optional] Create node pool for exporting annual npp and world pop.
   
    ```bash
    gcloud container node-pools create highmem-node-pool \
    --cluster=${GKE_CLUSTER} \
    --machine-type=n1-highmem-8 --disk-size 1500GB \
    --num-nodes=1
    ```
   
4. Create GCP Service Account with key file and create Kubernetes secret:
    ```bash
    gcloud container clusters get-credentials {CLUSTER_ID} --zone {ZONE} --project ${PROJECT}
    kubectl create secret generic service-account --from-file service-account.json={SERVICE_ACCOUNT_KEY.json} 
    ```

5. Follow the steps in [Configuring Airflow Variables](#configuring-airflow-variables) to configure Airflow variables.
    
6. Follow the steps in [Deploying Airflow DAGs](#deploying-airflow-dags) 
to deploy Airflow DAGs to Cloud Composer Environment.
 
7. Follow the steps [here](https://cloud.google.com/composer/docs/how-to/managing/creating#notification) 
to configure email notifications.

## Configuring Airflow Variables

- Clone geo weather Airflow: `git clone https://github.com/gcp-pdp/geo-earthengine && cd geo-earthengine/airflow`.
- Copy `example_airflow_variables.json` to `airflow_variables.json`. 
  Edit `airflow_variables.json` and update configuration options with your values. 
  You can find variables description in the table below. For the `output_bucket` variable 
  specify the bucket created on step 1 above. You can get it by running `echo $BUCKET`.
- Open Airflow UI. You can get its URL from `airflowUri` configuration option: 
  `gcloud composer environments describe ${ENVIRONMENT_NAME} --location us-central1`.
- Navigate to **Admin > Variables** in the Airflow UI, click **Choose File**, select `airflow_variables.json`, 
  and click **Import Variables**.
  
### Airflow Variables

| Variable | Description |
|---|---|
| `output_bucket` | GCS bucket where exported files with blockchain data will be stored |
| `export_start_date` | export start date, default: `2019-04-22` |
| `export_end_date` | export end date, used for integration testing, default: None |
| `export_schedule_interval` | export cron schedule, default: `0 1 * * *` |
| `image_name` | exporter docker image name, default: `gcr.io/gcp-pdp-weather-dev/geo-exporter` |
| `image_version` | exporter docker image version |
| `image_pull_policy` | exporter docker image pull policy, default: `Always` |
| `namespace` | namespace that exporter pod will be running in, default: `default` |
| `resources` | exporter container resources |
| `node_selector` | node-pool label that exporter pod will be scheduled, default: `default-pool` |
| `excluded_images` | comma separated list of images that will not be exported |
| `notification_emails` | comma-separated list of emails where notifications on DAG failures, retries and successes will be delivered. |
| `export_max_active_runs` | max active DAG runs for export, default: `3` |
| `destination_dataset_project_id` | The project id where destination BigQuery dataset is |
| `destination_dataset_name` | The destination BigQuery dataset name |
| `load_schedule_interval` | load cron schedule, default: `0 2 * * *` |
| `load_end_date` | load end date, used for integration testing, default: None |
  
## Deploying Airflow DAGs

- Get the value from `dagGcsPrefix` configuration option from the output of:
  `gcloud composer environments describe ${ENVIRONMENT_NAME} --location us-central1`.
- Upload DAGs to the bucket. Make sure to replace `<dag_gcs_prefix>` with the value from the previous step:
  `./upload_dags.sh <dag_gcs_prefix>`.
- To understand more about how the Airflow DAGs are structured 
  read [this article](https://cloud.google.com/blog/products/data-analytics/ethereum-bigquery-how-we-built-dataset).
- Note that it will take one or more days for `export_dag` to finish exporting the historical data.

## Troubleshooting

To troubleshoot issues with Airflow tasks use **View Log** button in the Airflow console for individual tasks.
Read [Airflow UI overview](https://airflow.apache.org/docs/stable/ui.html) and 
[Troubleshooting DAGs](https://cloud.google.com/composer/docs/how-to/using/troubleshooting-dags) for more info. 
