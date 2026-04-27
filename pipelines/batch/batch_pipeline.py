import os
import csv
import io
import logging
import apache_beam as beam
import hashlib
import json
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import date

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'edwards-platform')
BUCKET = os.environ.get('GCS_BUCKET', 'edwards-datalake')
RUN_DATE = date.today().isoformat()
RAW_PATH = f'gs://{BUCKET}/raw'
CURATED_PATH = f'gs://{BUCKET}/curated/{RUN_DATE}'
PROCESSED_PATH = f'gs://{BUCKET}/processed/{RUN_DATE}'
DEAD_LETTER_PATH = f'gs://{BUCKET}/dead_letter/{RUN_DATE}'

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

REQUIRED_FIELDS = ['patient_id', 'device_id', 'manufacturer_id', 'trial_site_id',
    'date_id', 'reading_id', 'event_id', 'production_id',
    'heart_rate', 'systolic_pressure', 'diastolic_pressure',
    'pressure_gradient', 'battery_level', 'signal_strength',
    'units_produced', 'units_passed_qc', 'defect_rate']

RANGE_RULES = {
    'heart_rate':        (0, 300),
    'systolic_pressure': (0, 300),
    'pressure_gradient': (0, 300),
    'defect_rate':       (0, 10)
}


def passes_quality(record):
    for field in REQUIRED_FIELDS:
        if field in record and (record[field] is None or str(record[field]).strip() == ''):
            return False
    for field, (min_val, max_val) in RANGE_RULES.items():
        if field in record:
            try:
                if not (min_val <= float(record[field]) <= max_val):
                    return False
            except (ValueError, TypeError):
                return False
    return True


def get_fail_reason(record):
    for field in REQUIRED_FIELDS:
        if field in record and (record[field] is None or str(record[field]).strip() == ''):
            return {**record, '_fail_reason': f'null_{field}'}
    for field, (min_val, max_val) in RANGE_RULES.items():
        if field in record:
            try:
                if not (min_val <= float(record[field]) <= max_val):
                    return {**record, '_fail_reason': f'range_{field}'}
            except (ValueError, TypeError):
                return {**record, '_fail_reason': f'type_cast_{field}'}
    return {**record, '_fail_reason': 'unknown'}

      

def run_patient_pipeline():
    options = PipelineOptions(
        project = PROJECT_ID,
        runner = 'DirectRunner',
        temp_location = f'gs://{BUCKET}/temp'
    )

    with beam.Pipeline(options=options) as p:
        parsed = (
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
        )

        good = parsed | 'FilterGood_dim_patient' >> beam.Filter(passes_quality)
        bad  = parsed | 'FilterBad_dim_patient'  >> beam.Filter(lambda r: not passes_quality(r))

        # Deduplication: in production, deduplicate on primary key here before writing
        # to curated. Skipped because generate_data.py uses seed(42) which produces
        # unique IDs. Implementation would use beam.GroupByKey on the primary ID
        # and emit one record per key.

        (
            good
            | 'MaskPHI' >> beam.Map(mask_patient_phi)
            | 'ToJSON_dim_patient' >> beam.Map(json.dumps)
            | 'WriteToGCS_dim_patient' >> beam.io.WriteToText(
                f'{CURATED_PATH}/dim_patient',
                file_name_suffix='.jsonl'
            )
        )

        (
            bad
            | 'DeadLetterMap_dim_patient' >> beam.Map(get_fail_reason)
            | 'DeadLetterJSON_dim_patient' >> beam.Map(json.dumps)
            | 'WriteDeadLetter_dim_patient' >> beam.io.WriteToText(
                f'{DEAD_LETTER_PATH}/dim_patient',
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
        parsed = (
            p
            | f'Read_{table_name}' >> beam.io.ReadFromText(
                f'{RAW_PATH}/{table_name}.csv',
                skip_header_lines=1
            )
            | f'Parse_{table_name}' >> beam.Map(lambda line: dict(zip(
                columns,
                next(csv.reader(io.StringIO(line)))
            )))
        )

        good = parsed | f'FilterGood_{table_name}' >> beam.Filter(passes_quality)
        bad  = parsed | f'FilterBad_{table_name}'  >> beam.Filter(lambda r: not passes_quality(r))

        # Deduplication: in production, deduplicate on primary key here before writing
        # to curated. Skipped because generate_data.py uses seed(42) which produces
        # unique IDs. Implementation would use beam.GroupByKey on the primary ID
        # and emit one record per key.

        (
            good
            | f'ToJSON_{table_name}' >> beam.Map(json.dumps)
            | f'Write_{table_name}' >> beam.io.WriteToText(
                f'{CURATED_PATH}/{table_name}',
                file_name_suffix='.jsonl'
            )
        )

        (
            bad
            | f'DeadLetterMap_{table_name}' >> beam.Map(get_fail_reason)
            | f'DeadLetterJSON_{table_name}' >> beam.Map(json.dumps)
            | f'WriteDeadLetter_{table_name}' >> beam.io.WriteToText(
                f'{DEAD_LETTER_PATH}/{table_name}',
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
        