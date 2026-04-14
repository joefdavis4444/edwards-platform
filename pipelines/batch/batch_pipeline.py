import os
import csv
import io
import logging
import apache_beam as beam
import hashlib
import json
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'edwards-platform')
BUCKET = os.environ.get('GCS_BUCKET', 'edwards-datalake')
RAW_PATH = f'gs://{BUCKET}/raw'
CURATED_PATH =f'gs://{BUCKET}/curated'
PROCESSED_PATH = f'gs://{BUCKET}/processed'

logging.basicConfig(level=logging.INFO)

def mask_patient_phi(record):
    token = hashlib.sha256(
        f"{record['first_name']}{record['last_name']}{record['ssn_last4']}".encode()
    ).hexdigest()[:16]

    return {
        'patient_id': record['patient_id'],
        'patient_token': token,
        'age_group': record['age_group'],
        'trial_site_id': record['trial_site_id'],
        'diagnosis': record['diagnosis'],
        'severity_level': record['severity_level'],
        'treatment': record['treatment'],
        'enrollment_status': record['enrollment_status']
    }

def run_patient_pipeline():
    options = PipelineOptions(
        project = PROJECT_ID,
        runner = 'DirectRunner',
        temp_location = f'gs://{BUCKET}/temp'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadPatients' >> beam.io.ReadFromText(
                f'{RAW_PATH}/dim_patient.csv',
                skip_header_lines=1
            )
            | 'ParseCSV' >> beam.Map(lambda line: dict(zip(
                ['patient_id', 'first_name', 'last_name', 'exact_age', 'age_group',
                 'address', 'ssn_last4', 'trial_site_id', 'diagnosis',
                 'severity_level', 'treatment', 'enrollment_status'],
                next(csv.reader(io.StringIO(line)))
            )))
            | 'MaskPHI' >> beam.Map(mask_patient_phi)
            | 'ToJSON' >> beam.Map(json.dumps)
            | 'WriteToGCS' >> beam.io.WriteToText(
                f'{CURATED_PATH}/dim_patient',
                file_name_suffix='.jsonl'
            )

        )

if __name__ == '__main__':
    logging.info('Starting batch pipeline...')
    run_patient_pipeline()
    logging.info('Pipeline complete.')
        