# Google Books Ngrams ETL Dataflow

## Prerequisites

* linux/macos terminal 
* git
* [gcloud](https://cloud.google.com/sdk/install)

## Running

### Setup

Create GCS bucket to hold dataflow templates and temp files
```bash
PROJECT=$(gcloud config get-value project 2> /dev/null)
BUCKET=${PROJECT}-dataflow
gsutil mb gs://${BUCKET}/
```

### Local

```bash
TEMP_LOCATION="gs://${BUCKET}/temp"
python -m beam \
--temp_location $TEMP_LOCATION \
--project $PROJECT \
--input-file $GCS_PATH \
--output-table "$PROJECT:$DATASET.$TABLE" \
--output-schema $SCHEMA_FILE
```

### Dataflow

Build docker image
```bash
TEMPLATE_IMAGE="gcr.io/$PROJECT/ngrams-beam:latest"
gcloud builds submit --tag $TEMPLATE_IMAGE .
```
   
Create Dataflow flex template
```bash
TEMPLATE_PATH="gs://$BUCKET/templates/ngrams-beam.json"
gcloud dataflow flex-template build $TEMPLATE_PATH \
 --image "$TEMPLATE_IMAGE" \
 --sdk-language "PYTHON" \
 --metadata-file "metadata.json"
```

Run flex template
```bash
REGION="us-central1"
gcloud dataflow flex-template run "load-parquet-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input-file="$GCS_PATH" \
    --parameters output-table="$PROJECT:$DATASET.$TABLE" \
    --parameters output-schema="/dataflow/template/schemas/world_pop.json" \
    --region "$REGION"
```