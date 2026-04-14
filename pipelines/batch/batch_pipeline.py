import os
import logging
import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'edwards-platform')
BUCKET = os.environ.get('GCS_BUCKET', 'edwards-datalake')
RAW_PATH = f'gs://{BUCKET}/raw'
CURATED_PATH =f'gs//{BUCKET}/curated'
PROCESSED_PATH = f'gs//{BUCKET}/processed'

logging.basicConfig(level=logging.INFO)