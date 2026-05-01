import os
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from datetime import date

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'edwards-platform')
BUCKET = os.environ.get('EDWARDS_BUCKET', 'edwards-datalake')
DATASET = 'edwards_dw'
RUN_DATE = date.today().isoformat()
CURATED_PATH = f'gs://{BUCKET}/curated/{RUN_DATE}'
SCHEMA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../infrastructure/schema')

logging.basicConfig(level=logging.INFO)


def load_schema(table_name):
    schema_path = os.path.join(SCHEMA_DIR, f'{table_name}.json')
    with open(schema_path) as f:
        return json.load(f)


def coerce_types(record, schema_fields):
    """Cast values read from JSONL to the Python types BigQuery expects."""
    result = {}
    for field in schema_fields:
        name = field['name']
        if name not in record:
            continue
        val = record[name]
        if val is None or val == '':
            result[name] = None
            continue
        ftype = field['type']
        try:
            if ftype == 'INTEGER':
                result[name] = int(float(val))
            elif ftype == 'FLOAT':
                result[name] = float(val)
            elif ftype == 'BOOLEAN':
                if isinstance(val, bool):
                    result[name] = val
                else:
                    result[name] = str(val).lower() in ('true', '1', 'yes')
            else:
                # STRING, DATE, TIMESTAMP — pass through as-is
                result[name] = val
        except (ValueError, TypeError):
            result[name] = None
    return result


def run_table_load(table_name, write_disposition):
    schema = load_schema(table_name)

    # BigQuery schema dict format — preserves REQUIRED/NULLABLE mode
    bq_schema = {'fields': schema}

    options = PipelineOptions(
        project=PROJECT_ID,
        runner='DirectRunner',
        temp_location=f'gs://{BUCKET}/temp'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | f'Read_{table_name}' >> beam.io.ReadFromText(
                f'{CURATED_PATH}/{table_name}*.jsonl'
            )
            | f'Parse_{table_name}' >> beam.Map(json.loads)
            | f'CoerceTypes_{table_name}' >> beam.Map(
                coerce_types, schema_fields=schema
            )
            | f'WriteToBQ_{table_name}' >> WriteToBigQuery(
                table=f'{PROJECT_ID}:{DATASET}.{table_name}',
                schema=bq_schema,
                write_disposition=write_disposition,
                create_disposition=BigQueryDisposition.CREATE_NEVER
            )
        )


# Dimensions: WRITE_TRUNCATE — idempotent, always load the full current snapshot.
# Reload runs overwrite the table so reruns are safe.
DIMENSION_TABLES = [
    'dim_manufacturer',
    'dim_device',
    'dim_trial_site',
    'dim_patient',
    'dim_date',
]

# Facts: WRITE_APPEND — batch records stack alongside streaming records.
# Each daily run adds the day's curated rows without wiping streaming history.
FACT_TABLES = [
    'fct_device_readings',
    'fct_clinical_events',
    'fct_manufacturing',
]


if __name__ == '__main__':
    print(f'Starting BQ load pipeline for {RUN_DATE}...')

    for table in DIMENSION_TABLES:
        print(f'  Loading {table} → WRITE_TRUNCATE...')
        run_table_load(table, BigQueryDisposition.WRITE_TRUNCATE)

    for table in FACT_TABLES:
        print(f'  Loading {table} → WRITE_APPEND...')
        run_table_load(table, BigQueryDisposition.WRITE_APPEND)

    print('BQ load pipeline complete.')
