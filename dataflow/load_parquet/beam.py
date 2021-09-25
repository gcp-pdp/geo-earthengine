import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def read_bigquery_schema_from_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return json.loads(content)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file",
        required=True,
        help="The file path for the input parquet files.",
    )
    parser.add_argument(
        "--output-table", required=True, help="The BigQuery table to write."
    )
    parser.add_argument(
        "--output-schema", required=True, help="The BigQuery table schema."
    )
    args, beam_args = parser.parse_known_args()

    table_schema = {"fields": read_bigquery_schema_from_file(args.output_schema)}

    beam_options = PipelineOptions(beam_args)
    with beam.Pipeline(options=beam_options) as pipeline:
        records = (
            pipeline
            | "Read files" >> beam.io.ReadFromParquet(args.input_file)
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
