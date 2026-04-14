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

def run_pipeline(table_name, columns):
    options = PipelineOptions(
        project=PROJECT_ID,
        runner='DirectRunner',
        temp_location=f'gs://{BUCKET}/temp'
    )
    with beam.Pipeline(options=options) as p:
        (
            p
            | f'Read_{table_name}' >> beam.io.ReadFromText(
                f'{RAW_PATH}/{table_name}.csv',
                skip_header_lines=1
            )
            | f'Parse_{table_name}' >> beam.Map(lambda line: dict(zip(
                columns,
                next(csv.reader(io.StringIO(line)))
            )))
            | f'ToJSON_{table_name}' >> beam.Map(json.dumps)
            | f'Write_{table_name}' >> beam.io.WriteToText(
                f'{CURATED_PATH}/{table_name}',
                file_name_suffix='.jsonl'
            )
        )


DIMENSION_TABLES = {
    'dim_manufacturer': ['manufacturer_id', 'manufacturer_name', 'city', 'country', 'region', 'fda_approved', 'certification_status'],
    'dim_device': ['device_id', 'device_model', 'device_type', 'firmware_version', 'status', 'manufacturer_id'],
    'dim_trial_site': ['trial_site_id', 'site_name', 'city', 'country', 'region', 'status'],
    'dim_date': ['date_id', 'full_date', 'year', 'month', 'day', 'quarter', 'day_of_week', 'is_holiday', 'holiday_name'],
}

FACT_TABLES = {
    'fct_device_readings': ['reading_id', 'device_id', 'patient_id', 'trial_site_id','date_id','timestamp','reading_type','heart_rate','systolic_pressure','diastolic_pressure','pressure_gradient','battery_level','signal_strength' ],
    'fct_clinical_events': ['event_id','patient_id','trial_site_id','date_id','timestamp','event_type','outcome','physician_notes','follow_up_required'],
    'fct_manufacturing': ['production_id','device_id','manufacturer_id','date_id','units_produced','units_passed_qc','defect_rate','production_status','batch_number']
}


if __name__ == '__main__':
    print('Starting batch pipeline...')
    run_patient_pipeline()
    for table, columns in DIMENSION_TABLES.items():
        print(f'Processing {table}...')
        run_pipeline(table, columns)
    for table, columns in FACT_TABLES.items():
        print(f'Processing {table}...')
        run_pipeline(table, columns)
    print('Pipeline complete.')
        